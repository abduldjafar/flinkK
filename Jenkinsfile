pipeline {
  agent any
  stages {
    stage('list'){
      steps {
        sh 'ls -al'
        sh 'pwd'
      }
    }
    stage('Build') {
      steps {
        sh 'mvn -B -DskipTests clean package'
      }
    }
  }
}
