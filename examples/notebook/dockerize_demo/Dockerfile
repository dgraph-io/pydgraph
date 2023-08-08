# Use the Python base image
FROM python:3.9

# Install Dgraph standalone v23.0.0
RUN wget https://github.com/dgraph-io/dgraph/releases/download/v23.0.0/dgraph-linux-arm64.tar.gz && \
    tar xvfz dgraph-linux-arm64.tar.gz && \
    mv dgraph /usr/local/bin/

# Set the working directory
WORKDIR /app

# Copy the Pipfile and Pipfile.lock to the container
COPY Pipfile* /app/

# Install pipenv
RUN pip install --no-cache-dir pipenv

# Install project dependencies using pipenv
RUN pipenv install


# Copy the schema file to the container
COPY schema.graphql /app/
COPY schema-flagged.graphql /app/
COPY example.env /app/

# Copy the notebook files to the container
COPY notebook.ipynb /app/
COPY workshop.ipynb /app/

# Copy the data files to the container
COPY data.rdf.gz /app/

#copy utils
COPY utils.py /app/

# Expose the necessary ports
EXPOSE 8888 8080

# Start Dgraph standalone, load data, apply the schema, and start Jupyter
CMD ["sh", "-c", "dgraph zero & dgraph alpha & sleep 20 && curl -X POST localhost:8080/admin/schema --data-binary '@schema.graphql' && dgraph live -f /app/data.rdf.gz --alpha localhost:9080 && pipenv run jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"]
