# ── Base image ────────────────────────────────────────────────────────────────
# Python 3.11 on Debian slim — small footprint, no desktop GUI bloat
FROM python:3.11-slim

# ── System dependencies ───────────────────────────────────────────────────────
# PySpark requires Java. openjdk-17-jre-headless is the server-side JRE
# (no GUI, smaller than the full JDK).
# --no-install-recommends keeps the image lean by skipping optional packages.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Tell PySpark where Java lives
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# ── Python dependencies ───────────────────────────────────────────────────────
WORKDIR /app

# Copy requirements first so Docker can cache this layer.
# If only your code changes (not requirements.txt), pip install is skipped on rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ──────────────────────────────────────────────────────────
COPY . .

# ── Entry point ───────────────────────────────────────────────────────────────
# ENTRYPOINT sets the base command. CMD provides the default sub-command.
# This lets you run: docker run heat-cold heatwave --start-year 2003 ...
#                or: docker run heat-cold coldwave --start-year 2012 ...
ENTRYPOINT ["python", "heatwave.py"]
CMD ["--help"]
