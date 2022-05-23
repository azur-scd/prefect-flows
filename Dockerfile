FROM prefecthq/prefect:latest

ARG PREFECT_API_KEY

RUN /usr/local/bin/python -m pip install --upgrade pip
WORKDIR /opt/prefect
COPY . /opt/prefect/
VOLUME ["/opt/prefect"]
RUN pip install pandas requests beautifulsoup4 lxml
#ou pip install -r requirements.txt
RUN prefect backend cloud
RUN prefect auth login --key ${PREFECT_API_KEY}
CMD ["./prefect-start.sh"]