# curate no cabe en un zip normal de Lambda: polars + reverse_geocoder
# (que arrastra numpy/scipy) pesan 367-434MB descomprimidos, por encima del
# limite de 250MB de zip/layers. Por eso imagen de contenedor (hasta 10GB).
# Ver docs/decisiones.md para el detalle medido.
FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY handler.py geo.py country_regions.json ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
