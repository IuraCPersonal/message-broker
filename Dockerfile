FROM elixir:1.14

RUN mkdir /app
WORKDIR /app

COPY . .

RUN mix local.hex --force
RUN mix deps.get
RUN mix deps.compile

CMD ["mix", "run", "--no-halt"]

# docker build -t ptrlab:latest . 
# docker run -p 8080:8080 -p 8081:8081 ptrlab:latest
