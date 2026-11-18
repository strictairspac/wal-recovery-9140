"""Quiescent-checkpoint WAL teaching model with strict write locks, nested savepoints and redo/undo."""
import copy,hashlib,json,re
SP_NAME=r'[A-Za-z0-9_-]{1,30}'

def validate(initial,records):
    if not initial or len(initial)>30 or not records or len(records)>200: raise ValueError('需1—30个键及1—200条日志')
    for key,value in initial.items():
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,50}',key) or isinstance(value,bool) or not isinstance(value,int) or abs(value)>1000000000:
            raise ValueError('初始键为字母数字下划线短横线，值为绝对值不超过10亿的整数')
    state=copy.deepcopy(initial); active={};used=set();locks={};savepoints={}
    for lsn,r in enumerate(records,1):
        op=r.get('op');tx=r.get('tx')
        if op=='checkpoint':
            if active:raise ValueError(f'LSN {lsn}: 仅支持无活动事务的静止检查点')
            continue
        if not tx:raise ValueError(f'LSN {lsn}: 需要事务标识')
        if op=='begin':
            if tx in used:raise ValueError(f'LSN {lsn}: 事务标识不得重复')
            active[tx]=[];used.add(tx);savepoints[tx]=[]
        elif op=='write':
            if tx not in active:raise ValueError(f'LSN {lsn}: 写入事务尚未begin')
            key=r.get('key');value=r.get('value')
            if key not in initial:raise ValueError(f'LSN {lsn}: 写入键必须存在于初始账本')
            if isinstance(value,bool) or not isinstance(value,int) or abs(value)>1000000000:raise ValueError(f'LSN {lsn}: 数值超限')
            if key in locks and locks[key]!=tx:raise ValueError(f'LSN {lsn}: 违反严格写锁，活动事务持有同键')
            locks[key]=tx;active[tx].append((key,state[key]));state[key]=value
        elif op=='savepoint':
            if tx not in active:raise ValueError(f'LSN {lsn}: 保存点事务尚未begin')
            name=r.get('name')
            if not name or not re.fullmatch(SP_NAME,name):raise ValueError(f'LSN {lsn}: 保存点名称为1—30位字母数字下划线短横线')
            sps=savepoints[tx];sps[:]=[s for s in sps if s['name']!=name];sps.append({'name':name,'pos':len(active[tx])})
        elif op=='rollback':
            if tx not in active:raise ValueError(f'LSN {lsn}: 回滚事务尚未begin')
            sps=savepoints[tx];idx=next((i for i,s in enumerate(sps) if s['name']==r.get('name')),None)
            if idx is None:raise ValueError(f'LSN {lsn}: 保存点不存在或已随外层回滚失效')
            pos=sps[idx]['pos'];undone=active[tx][pos:]
            for key,before in reversed(undone):state[key]=before
            del active[tx][pos:];del sps[idx+1:]
            # Locks survive only where the transaction still holds a live write; keys occupied solely by undone writes are released.
            held={k for k,_ in active[tx]}
            for key in {k for k,_ in undone}-held:
                if locks.get(key)==tx:del locks[key]
        elif op in ('commit','abort'):
            if tx not in active:raise ValueError(f'LSN {lsn}: 事务未开启或已结束')
            if op=='abort':
                for key,before in reversed(active[tx]):state[key]=before
            del active[tx];del savepoints[tx]
            locks={k:v for k,v in locks.items() if v!=tx}
        else:raise ValueError(f'LSN {lsn}: 未知操作')
    return True

def recover(initial,records,crash_after):
    validate(initial,records)
    if not isinstance(crash_after,int) or not 0<=crash_after<=len(records):raise ValueError('崩溃点超出日志范围')
    prefix=records[:crash_after]; state=copy.deepcopy(initial); active={};savepoints={}; winners=[];aborted=[];checkpoint=0;durable=copy.deepcopy(initial);trace=[];committed_writes=[]
    for lsn,r in enumerate(prefix,1):
        op=r['op'];tx=r.get('tx')
        if op=='begin':active[tx]=[];savepoints[tx]=[]
        elif op=='write':
            row={'lsn':lsn,'tx':tx,'key':r['key'],'before':state[r['key']],'after':r['value']}
            active[tx].append(row);state[r['key']]=r['value']
            trace.append(dict(row,phase='redo'))
        elif op=='savepoint':
            sps=savepoints[tx];sps[:]=[s for s in sps if s['name']!=r['name']];sps.append({'name':r['name'],'pos':len(active[tx])})
        elif op=='rollback':
            sps=savepoints[tx];idx=next(i for i,s in enumerate(sps) if s['name']==r['name']);pos=sps[idx]['pos']
            # Repeating history applies the rollback itself, so already-undone writes are never resurrected.
            for row in reversed(active[tx][pos:]):
                state[row['key']]=row['before']
                trace.append(dict(row,phase='redo_rollback_undo',savepoint=r['name'],via_lsn=lsn))
            del active[tx][pos:];del sps[idx+1:]
        elif op=='abort':
            for row in reversed(active[tx]):
                state[row['key']]=row['before'];trace.append(dict(row,phase='redo_abort_undo'))
            del active[tx];del savepoints[tx];aborted.append(tx)
        elif op=='commit':
            winners.append(tx);committed_writes.extend(active[tx]);del active[tx];del savepoints[tx]
        elif op=='checkpoint':
            checkpoint=lsn;durable=copy.deepcopy(state);trace=[];committed_writes=[];winners=[];aborted=[]
    # Simulated crash durable pages are exactly the last quiescent checkpoint.
    # Recovery repeats history after that point, then undoes the surviving writes of all unfinished transactions.
    before_undo=copy.deepcopy(state);losers=sorted(active)
    undo=sorted([w for tx in active for w in active[tx]],key=lambda w:w['lsn'],reverse=True)
    for row in undo:
        state[row['key']]=row['before'];trace.append(dict(row,phase='undo_loser'))
    # Independent winner-only oracle from durable checkpoint applies surviving writes of committed transactions.
    oracle=copy.deepcopy(durable)
    for row in sorted(committed_writes,key=lambda w:w['lsn']):oracle[row['key']]=row['after']
    checksum=hashlib.sha256(json.dumps(state,sort_keys=True).encode()).hexdigest()
    source=json.dumps({'initial':initial,'records':records,'crash_after':crash_after},sort_keys=True)
    return {'simulation':True,'model_version':'strict-wal-savepoint-v2','crash_after':crash_after,'checkpoint_lsn':checkpoint,
            'durable_checkpoint':durable,'after_redo':before_undo,'recovered':state,'oracle':oracle,'invariant_passed':state==oracle,
            'committed':winners,'aborted':aborted,'losers':losers,'redo_count':len([t for t in trace if t['phase']=='redo']),
            'undo_count':len(undo),'rollback_undo_count':len([t for t in trace if t['phase']=='redo_rollback_undo']),
            'trace':trace,'result_sha256':checksum,'input_sha256':hashlib.sha256(source.encode()).hexdigest(),
            'boundary':'Python WAL教学模型；严格写锁、嵌套保存点、静止检查点、整数键值、日志前缀持久。没有注入真实PostgreSQL崩溃，未执行数据库物理恢复。'}
