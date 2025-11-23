FROM alpine:3.19

RUN apk add --no-cache curl jq

COPY start_metabase.sh /start_metabase.sh
RUN chmod +x /start_metabase.sh

CMD [ "/start_metabase.sh" ]