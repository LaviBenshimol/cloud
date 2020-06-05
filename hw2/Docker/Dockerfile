FROM alpine:3.11.5
RUN apk add iperf3 
# Inform Docker that the container is listening on the specified port at runtime.
EXPOSE 5050
# Run the specified command within the container.
CMD iperf3 -s -p 5050



