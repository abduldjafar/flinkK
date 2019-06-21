pipeline {
  agent any
  stages {
    stage('list'){
      steps {
        sh 'ls -al'
      }
    }
    stage('Build') {
      steps {
        sh 'mvn -B -DskipTests clean package'
      }
    }
  }
}
