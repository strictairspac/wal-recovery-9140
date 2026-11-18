import os
from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.models.entities import User,LogSet,CrashCase
from src.utils.security import hash_password
RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},{'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},{'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},{'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'}]
SP_RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':110},{'op':'savepoint','tx':'T1','name':'sp1'},{'op':'write','tx':'T1','key':'sample_B','value':60},{'op':'savepoint','tx':'T1','name':'sp2'},{'op':'write','tx':'T1','key':'sample_A','value':999},{'op':'rollback','tx':'T1','name':'sp2'},{'op':'write','tx':'T1','key':'sample_A','value':130},{'op':'savepoint','tx':'T1','name':'sp3'},{'op':'write','tx':'T1','key':'sample_B','value':999},{'op':'rollback','tx':'T1','name':'sp1'},{'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':80},{'op':'commit','tx':'T2'},{'op':'write','tx':'T1','key':'sample_B','value':65},{'op':'commit','tx':'T1'}]
def seed():
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower()!='true':return
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@haili.local'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('Recovery2026!')))
        logs=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-晚班'))
        if logs is None:
            logs=LogSet(name='海砾采样账本-晚班',description='A初始100份、B初始50份，T1提交后检查点，T2未提交，T3已提交',initial={'sample_A':100,'sample_B':50},records=RECORDS);db.add(logs);db.flush()
        for name,point in [('断电点10-撤销T2',10),('完整提交点11',11),('初始点0',0)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=logs.id,crash_after=point))
        sp=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-保存点演练'))
        if sp is None:
            sp=LogSet(name='海砾采样账本-保存点演练',description='T1嵌套保存点sp1/sp2/sp3，回滚外层撤掉写错段、内层随之失效并释放B锁，T2接续写入提交',initial={'sample_A':100,'sample_B':50},records=SP_RECORDS);db.add(sp);db.flush()
        for name,point in [('保存点断电点14-撤T1保T2',14),('保存点完整提交点16',16)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=sp.id,crash_after=point))
        db.commit()
if __name__=='__main__':seed()
