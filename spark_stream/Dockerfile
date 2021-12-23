FROM andreper/jupyterlab:latest

WORKDIR /opt/workspace

ADD ./ ./

CMD ["spark-submit", "--jars", "jar/sqljdbc42.jar", "app.py"]
