# Base image
# FROM python:3.9

# # 작업 디렉토리 설정
# WORKDIR /app

# # 필요한 파일 복사
# COPY requirements.txt .
# COPY config.ini .
# COPY busan_pilot_forecast.py .

# # 필요한 패키지 설치
# RUN pip install --no-cache-dir -r requirements.txt

# 컨테이너 실행 명령
# CMD ["python", "busan_pilot_forecast.py"]
FROM python:3.9
WORKDIR /usr/src
RUN apt-get -y update
RUN apt install wget
RUN apt install unzip  
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt -y install ./google-chrome-stable_current_amd64.deb
RUN pip install chromedriver-autoinstaller
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.ini .
COPY busan_pilot_forecast.py .
CMD [ "python", "busan_pilot_forecast.py" ]
