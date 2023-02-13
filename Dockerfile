FROM ubuntu

RUN mkdir scheduler

ADD ./Scheduler ./scheduler/Scheduler

CMD ["/scheduler/Scheduler"]
