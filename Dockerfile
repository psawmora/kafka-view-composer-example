FROM openjdk:8-jre-alpine

RUN apk update && apk add --no-cache libc6-compat
RUN ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME

COPY ./build/libs/* ./app.jar
CMD ["java","-jar","app.jar"]
