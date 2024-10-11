FROM centos

ENV JAVA_HOME /etc/alternatives/jre

ADD server-init.sh /
ADD server-start.sh /

RUN /bin/bash /server-init.sh

EXPOSE 6182

CMD /bin/bash /server-start.sh
