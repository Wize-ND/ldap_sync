FROM oraclelinux:8-slim

RUN  microdnf install oracle-instantclient-release-el8 && \
     microdnf install oracle-instantclient-basic && \
     microdnf install gcc  \
                      oracle-instantclient-release-el8 \
                      oracle-instantclient-basic \
                      openldap-devel \
                      python39 \
                      python39-libs \
                      python39-devel \
                      python39-pip \
                      python39-setuptools && \
     microdnf clean all

WORKDIR /

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD [ "python3", "main.py" ]