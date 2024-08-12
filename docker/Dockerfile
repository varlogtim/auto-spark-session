ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG WHL_FILE

COPY ../dist/${WHL_FILE} /tmp/${WHL_FILE}
RUN python -m pip install /tmp/${WHL_FILE}
