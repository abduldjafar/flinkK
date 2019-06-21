pipeline {
  agent any
  stages {
    stage('list') {
      steps {
        sh 'ls -al'
        sh 'pwd'
      }
    }
    stage('Build') {
      parallel {
        stage('Build') {
          steps {
            sh 'mvn -B -DskipTests clean package'
          }
        }
        stage('build2') {
          steps {
            sh 'echo "hai"'
          }
        }
      }
    }
  }
}