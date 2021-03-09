from datetime import datetime
from typing import Optional

from rx.core import typing
from rx.disposable import MultipleAssignmentDisposable

from rxbp.schedulers.timeoutscheduler import TimeoutScheduler


def _interval(period: typing.RelativeTime,
              scheduler: Optional[typing.Scheduler] = None
              ) -> typing.Subscription:
    return _timer(period, period, scheduler)


def _timer(duetime: typing.AbsoluteOrRelativeTime,
           period: Optional[typing.RelativeTime] = None,
           scheduler: Optional[typing.Scheduler] = None
           ) -> typing.Subscription:
    if isinstance(duetime, datetime):
        if period is None:
            return observable_timer_date(duetime, scheduler)
        else:
            return observable_timer_duetime_and_period(duetime, period, scheduler)

    if period is None:
        return observable_timer_timespan(duetime, scheduler)

    return observable_timer_timespan_and_period(duetime, period, scheduler)


def observable_timer_date(duetime,
                          scheduler: Optional[typing.Scheduler] = None
                          ) -> typing.Subscription:
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_
        if not hasattr(_scheduler, "schedule_periodic"):
            _scheduler = TimeoutScheduler.singleton()

        def action(scheduler, state):
            observer.on_next([0])
            observer.on_completed()

        return _scheduler.schedule_absolute(duetime, action)

    return subscribe


def observable_timer_duetime_and_period(duetime,
                                        period,
                                        scheduler: Optional[typing.Scheduler] = None
                                        ) -> typing.Subscription:
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_
        if not hasattr(_scheduler, "schedule_periodic"):
            _scheduler = TimeoutScheduler.singleton()

        nonlocal duetime

        if not isinstance(duetime, datetime):
            duetime = _scheduler.now + _scheduler.to_timedelta(duetime)

        p = max(0.0, _scheduler.to_seconds(period))
        mad = MultipleAssignmentDisposable()
        dt = duetime
        count = 0

        def action(scheduler, state):
            nonlocal dt
            nonlocal count

            if p > 0.0:
                now = scheduler.now
                dt = dt + scheduler.to_timedelta(p)
                if dt <= now:
                    dt = now + scheduler.to_timedelta(p)

            observer.on_next([count])
            count += 1
            mad.disposable = scheduler.schedule_absolute(dt, action)

        mad.disposable = _scheduler.schedule_absolute(dt, action)
        return mad

    return subscribe


def observable_timer_timespan(duetime: typing.RelativeTime,
                              scheduler: Optional[typing.Scheduler] = None
                              ) -> typing.Subscription:
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_
        if not hasattr(_scheduler, "schedule_periodic"):
            _scheduler = TimeoutScheduler.singleton()

        d = _scheduler.to_seconds(duetime)

        def action(scheduler, state):
            observer.on_next([0])
            observer.on_completed()

        if d <= 0.0:
            return _scheduler.schedule(action)
        return _scheduler.schedule_relative(d, action)

    return subscribe


def observable_timer_timespan_and_period(duetime: typing.RelativeTime,
                                         period: typing.RelativeTime,
                                         scheduler: Optional[typing.Scheduler] = None
                                         ) -> typing.Subscription:
    if duetime == period:
        def subscribe(observer, scheduler_=None):
            _scheduler = scheduler or scheduler_
            if not hasattr(_scheduler, "schedule_periodic"):
                _scheduler = TimeoutScheduler.singleton()

            def action(count):
                observer.on_next([count])
                return count + 1

            return _scheduler.schedule_periodic(period, action, state=0)

        return subscribe

    return observable_timer_duetime_and_period(duetime, period, scheduler)
