FROM 172631448019.dkr.ecr.us-east-1.amazonaws.com/jorb:trusty-v11

RUN sudo apt-get --yes update && sudo DEBIAN_FRONTEND=noninteractive apt install -y  wget build-essential libssl-dev libffi-dev libxml2-dev \
    libxslt1-dev zlib1g-dev libkrb5-dev unzip openjdk-7-jdk slapd ldap-utils libsasl2-dev gcc wget openssh-server \
    rsync libsparkey-dev libcurl4-openssl-dev libldap2-dev gawk  postgresql=9.3+154ubuntu1.1 postgresql-contrib=9.3+154ubuntu1.1 \
    sqlite3 libsqlite3-dev libbz2-dev

# upgrade python to 2.7.13
RUN cd /tmp && wget https://www.python.org/ftp/python/2.7.13/Python-2.7.13.tgz && \
    tar xfz Python-2.7.13.tgz &&  cd Python-2.7.13/ && ./configure --enable-ipv6 && \
    make && sudo make install && python -m ensurepip --upgrade

RUN sudo apt-get --yes update && sudo DEBIAN_FRONTEND=noninteractive apt install -y python-dev \
    python-virtualenv=1.11.4-1ubuntu1 virtualenvwrapper=4.1.1-1 python-matplotlib

# mysql need at least 5.7
RUN echo mysql-apt-config mysql-apt-config/enable-repo select mysql-5.7-dmr | sudo debconf-set-selections && \
    wget http://dev.mysql.com/get/mysql-apt-config_0.2.1-1ubuntu12.04_all.deb && \
    sudo dpkg --install mysql-apt-config_0.2.1-1ubuntu12.04_all.deb && \
    sudo apt-get update -q && \
    sudo DEBIAN_FRONTEND=noninteractive  apt-get install --quiet --yes --force-yes mysql-server libmysqlclient-dev &&\
    sudo apt-get install -y postgresql=9.3+154ubuntu1.1 postgresql-contrib=9.3+154ubuntu1.1
# end mysql

RUN /jorb/scripts/install-redis.sh

RUN pip install --upgrade pip==9.0.1 && pip install --upgrade setuptools==33.1.1


ENV SLUGIFY_USES_TEXT_UNIDECODE yes
ENV TRAVIS_CACHE "/tmp"
ENV TRAVIS true
ENV HADOOP_DISTRO cdh
ENV HADOOP_HOME "/tmp/hadoop-cdh"
ENV HADOOP_OPTS "-D/tmp/krb5.conf"
ENV MINICLUSTER_HOME "/tmp/minicluster-1.1-SNAPSHOT"
ENV HIVE_HOME "/tmp/hive"
ENV PATH "${PATH}:/tmp/hive/bin"
ENV JAVA_HOME "/usr/lib/jvm/java-7-openjdk-amd64"
USER root
ENV USER root


ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN "mysql://root@localhost/airflow"

COPY ./1_10_4_docker_ci_requirements.txt /tmp/
RUN AIRFLOW_GPL_UNIDECODE=yes pip install --no-deps -r /tmp/1_10_4_docker_ci_requirements.txt

COPY ./download_mini_cluster.sh /tmp/
RUN /tmp/download_mini_cluster.sh

