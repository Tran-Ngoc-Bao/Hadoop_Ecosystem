FROM centos


ADD server-init.sh /
ADD server-start.sh /

RUN /bin/bash /server-init.sh

EXPOSE 5432

CMD /bin/bash /server-start.sh
