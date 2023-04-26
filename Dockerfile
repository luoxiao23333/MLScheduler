FROM ubuntu

RUN mkdir scheduler

#RUN apt update -y
#RUN apt install wget -y
#RUN mkdir go

#RUN cd go && wget "https://golang.google.cn/dl/go1.20.3.linux-amd64.tar.gz"
#RUN cd go && tar -xzf 'go1.20.3.linux-amd64.tar.gz'

#ADD export.sh /
#ADD *.mod /scheduler
#ADD *.go /scheduler
#ADD *.sum /scheduler
#ADD handler/ /scheduler
#ADD worker_pool/ /scheduler
ADD ./Scheduler ./scheduler/Scheduler

#RUN export GOPROXY=https://proxy.golang.com.cn,direct &&  export GOPATH=/go/go && export PATH=$PATH:/go/go/bin  &&  cd scheduler && go mod download

CMD ["/scheduler/Scheduler"]
