@Library('commerce_jenkins_master')_ 
import com.genco.commerce.build.EnvironmentConfiguration
import slackMsg
import groovy.json.JsonSlurperClassic
import groovy.io.*
import com.genco.commerce.build.Constants
import groovy.time.TimeCategory
import groovy.time.TimeDuration
java.io.File
java.io.Writer
java.io.OutputStream


properties = null
pipeline {
    agent {label 'WINDOWS_VM'}
    tools {
        maven 'Maven339'
        jdk 'JDK8'
    }
	environment {
        result =''
		recipient_email_ids = ""
		enableEmailNotifications ="false"
		isSlackEnabled ="false"
		slackChannel =""
		testTags = ""
		dev_testTags = ""
		test_testTags = ""
    }
    
    stages {
        /*stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                    echo "M2_HOME = ${M2_HOME}"
                '''
            }
        }*/
        stage ('Build') {
            steps {
            	echo "Test Tag Selected: ${TEST_TAG}"
				echo "ENV_NAME: ${ENV_NAME}"
               // bat 'mvn -f automation-test/pom.xml install -Dcucumber.options="--tags %TEST_TAG%"' 
               /* bat 'mvn -f automation-test/pom.xml install -Dcucumber.options="--tags @SmokeOne"'*/
               // cucumber fileIncludePattern: '**/target/cucumber-report.json', sortingMethod: 'ALPHABETICAL'
               // step([$class: 'CucumberReportPublisher', jsonReportDirectory: "automation-test/target/", jenkinsBasePath: '', fileIncludePattern: 'cucumber-report.json'])
                script{
				loadProperties()
				try{
						env_name = "${ENV_NAME}" ?: "TEST"
					}catch(e){ 
					echo "env_name in catch block "
					env_name = "TEST" 
					}
				try{
					echo "env_name : ${env_name}"
					echo " ENV_NAME: ${ENV_NAME}"
					
					if("DEV" == "${env_name}"){
						testTags = "${dev_testTags}"
					}else if("TEST" == "${env_name}"){
						testTags = "${test_testTags}"

		            }  
		             
					echo "Before test case execution"
					echo "testTags : ${testTags}"
					bat 'mvn -f Philips-automation-test/pom.xml install -Djob.name='+"${env.JOB_NAME}"+' -Dcucumber.options='+"${testTags}"
					echo "After test case execution"
					
					//echo "Before test report publish"
					//cucumber fileIncludePattern: '**/target/cucumber-report.json', sortingMethod: 'ALPHABETICAL'
					//step([$class: 'CucumberReportPublisher', jsonReportDirectory: "automation-test/target/", jenkinsBasePath: '', fileIncludePattern: 'cucumber-report.json'])
					//echo "After test report publish"
					
					//result = 'SUCCESS'
					}
					/*catch(e){
						result = 'FAILED'
					}
						echo "env_name : ${env_name}"
						echo "2.recipient_email_ids : ${recipient_email_ids}"
						sendNotifications("QA Automation ${result} in ${env_name}","${enableEmailNotifications}","${recipient_email_ids}","Version: ${env.BUILD_NUMBER} ${result}. : ${env.BUILD_URL}console Test tags :${testTags}")
						if(isSlackEnabled.toBoolean()){
							slackMsg  "QA Automation script execution in ${env_name} - Environment ${env.JOB_NAME} version: ${env.BUILD_NUMBER} ${result}. : (<${env.BUILD_URL}cucumber-html-reports/overview-features.html|Open>).","good","${slackChannel}"
						}
				 */
				 finally{
					echo "Before test report publish"
					cucumber fileIncludePattern: '**/target/cucumber-report.json', sortingMethod: 'ALPHABETICAL'
					step([$class: 'CucumberReportPublisher', jsonReportDirectory: "target/", jenkinsBasePath: '', fileIncludePattern: 'cucumber-report.json'])
					echo "After test report publish"
				 }
				}
			}
		}
	}
}

def sendNotifications(String status, String enableEmailNotifications, String recipient_email_ids, String emailBody) {
	if(enableEmailNotifications != null && enableEmailNotifications.toBoolean()){
		emailext body: "${emailBody}",
		recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']],
		subject: "${status}",
		to: "${recipient_email_ids}"
	}
}

def loadProperties() {
    node {
		echo "inside loadProperties..."
        checkout scm
        properties = new Properties()
		props1 = new Properties()
        File propertiesFile = new File("${workspace}/Philips-automation-test/application.properties")
        properties.load(propertiesFile.newDataInputStream())
        echo "properties file has been loaded..."
		recipient_email_ids = "${properties.recipient_email_ids}"
		enableEmailNotifications = "${properties.enableEmailNotifications}"
		isSlackEnabled = "${properties.isSlackEnabled}"
		slackChannel = "${properties.slackChannel}"
		dev_testTags = "${properties.DEV_testTags}"
		test_testTags = "${properties.TEST_testTags}"
		echo "recipient_email_ids : ${properties.recipient_email_ids}"
	    echo "enableEmailNotifications : ${properties.enableEmailNotifications}"
    }
}
