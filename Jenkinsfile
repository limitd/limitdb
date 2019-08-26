pipeline {
  agent {
    label 'crew-sre'
  }

  tools {
    nodejs '6.14.0'
  }

  options {
    timeout(time: 5, unit: 'MINUTES')
  }

  parameters {
    string(name: 'SlackTarget', defaultValue: '#sre-build', description: 'Target Slack Channel for notifications')
  }

  stages {
    stage('SharedLibs') { // Required. Stage to load the Auth0 shared library for Jenkinsfile
      steps {
        library identifier: 'auth0-jenkins-pipelines-library@master', retriever: modernSCM(
          [$class: 'GitSCMSource',
          remote: 'git@github.com:auth0/auth0-jenkins-pipelines-library.git',
          credentialsId: 'auth0extensions-ssh-key'])
      }
    }
    stage('Build') { // Build steps such as 'npm install' or launching containers
      steps {
        // Jenkins slaves don't share SSH keys with the master
        // Use the 'sshagent' step if you need to access auth0 GH private repositories within the pipeline
        sshagent(['auth0extensions-ssh-key']) {
          sh 'npm install'
        }
        // Find more examples of what to add here at https://github.com/auth0/auth0-users/blob/master/Jenkinsfile#L41
      }
    }
    stage('Lint') { // Testing steps such as running the linter, testing the Shrinkwrap file or running unit tests
      steps {
        script {
          try {
            sh 'npm run lint'
            githubNotify context: 'jenkinsfile/auth0/tests', description: 'Lint passed', status: 'SUCCESS'
          } catch (error) {
            githubNotify context: 'jenkinsfile/auth0/tests', description: 'Lint failed', status: 'FAILURE'
            throw error
          }
        }
      }
    }
    stage('Test') { // Testing steps such as running the linter, testing the Shrinkwrap file or running unit tests
      steps {
        script {
          withDockerRegistry(getArtifactoryRegistry()) {
            try {
              sh 'npm run test'
              githubNotify context: 'jenkinsfile/auth0/tests', description: 'Tests passed', status: 'SUCCESS'
            } catch (error) {
              githubNotify context: 'jenkinsfile/auth0/tests', description: 'Tests failed', status: 'FAILURE'
              throw error
            }
          }
        }
      }
    }
    stage('Report to SonarQube') {
      steps {
        script {
          def scannerHome = tool 'SonarQube Scanner 3.1';
          withSonarQubeEnv('Sonar') {
            sh "${scannerHome}/bin/sonar-scanner"
          }
        }
      }
    }
  }

  post {
    always { // Steps that need to run regardless of the job status, such as test results publishing, Slack notifications or dependencies cleanup
      script {
        notifySlack(params.SlackTarget, '');
      }

      // Recommended to clean the workspace after every run
      deleteDir()
    }
  }
}