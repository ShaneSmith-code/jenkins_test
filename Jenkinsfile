pipeline {
    agent any

    stages {
        stage('Linting') {
            steps {
                .\\linting\\lint_check.py data_management.py
            }
        }
       stage('Deploy') {
            steps {
                echo 'Deploy Successful'
            }
        }
    }
}