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
gcc Epoll.c -o epoll -pthread
./epoll &
SERVER_PID=$!
sleep 1
if ! kill -0 $SERVER_PID 2>/dev/null; then
  echo "Server failed to start or crashed immediately"
  exit 1
fi
kill -TERM $SERVER_PID
wait $SERVER_PID
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo "Server did not shut down cleanly, exit code: $EXIT_CODE"
  exit 1
fi
echo "Server started and shut down cleanly"
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
