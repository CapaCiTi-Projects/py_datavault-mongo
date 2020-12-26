import datetime as dtime


# date = dtime.date.today()
# time = dtime.datetime.min.time()
# dt = dtime.datetime.combine(date, time)
dt = dtime.datetime(2020, 1, 29)
utc = dt.utcnow()
print(dt)
print(dt.timestamp())
print(type(dt.utcnow()))

print(utc)
