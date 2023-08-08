Pre-Reqs: Docker
Step 1: Build the image

cd vlg/dockerize_demo
docker build --no-cache -t dshekhar_pandora_stdalone_v13 .


Step 2: Run the container. Make sure to forward 8888 since the notebook will be running from the container
docker run -p 8888:8888 dshekhar_pandora_stdalone_v13  

Step 3 (Optional): Prune your docker environment
docker image prune -a
docker network prune -a
docker system prune -a