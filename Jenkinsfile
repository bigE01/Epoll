pipeline {
    agent any
    environment {
        epoll_container = "epoll-image"
    }
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Build Docker Image') {
            steps {
                sh 'docker build -t ${epoll_container} .'
            }
        }
        stage('Compile & Run') {
    steps {
        timeout(time: 30, unit: 'SECONDS') {
            sh '''
docker run --rm -i ${epoll_container} sh <<'SCRIPT'
apt-get update && apt-get install -y curl
gcc Epoll.c -o epoll -pthread
./epoll &
SERVER_PID=$!
sleep 1
if ! kill -0 $SERVER_PID 2>/dev/null; then
  echo "Server failed to start or crashed immediately"
  exit 1
fi
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" localhost:8081/index.html)
if [ "$RESPONSE" != "200" ]; then
  echo "Expected 200 from index.html, got $RESPONSE"
  kill -TERM $SERVER_PID
  exit 1
fi
RESPONSE_404=$(curl -s -o /dev/null -w "%{http_code}" localhost:8081/nope.html)
if [ "$RESPONSE_404" != "404" ]; then
  echo "Expected 404 from nope.html, got $RESPONSE_404"
  kill -TERM $SERVER_PID
  exit 1
fi
kill -TERM $SERVER_PID
wait $SERVER_PID
echo "Server started, served requests correctly, and shut down cleanly"
SCRIPT
'''
        }
    }
}
    }
    post {
        always {
            sh 'docker image prune -f'
        }
        failure {
            echo 'Build failed — check console output above.'
        }
    }
}
