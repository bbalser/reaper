library(
    identifier: 'pipeline-lib@4.3.6',
    retriever: modernSCM([$class: 'GitSCMSource',
                          remote: 'https://github.com/SmartColumbusOS/pipeline-lib',
                          credentialsId: 'jenkins-github-user'])
)

properties([
    pipelineTriggers([scos.dailyBuildTrigger()]),
])

def image
def doStageIf = scos.&doStageIf
def doStageIfRelease = doStageIf.curry(scos.changeset.isRelease)
def doStageUnlessRelease = doStageIf.curry(!scos.changeset.isRelease)
def doStageIfPromoted = doStageIf.curry(scos.changeset.isMaster)

node ('infrastructure') {
    ansiColor('xterm') {
        scos.doCheckoutStage()

        imageTag = "${env.GIT_COMMIT_HASH}"

        doStageUnlessRelease('Build') {
            builderImage = docker.build("scos/reaper:${env.GIT_COMMIT_HASH}", "--target builder")
            image = docker.build("scos/reaper:${env.GIT_COMMIT_HASH}")

            builderImage.inside() {
                sh('''
                export HOST_IP=$(curl http://169.254.169.254/latest/meta-data/local-ipv4)
                MIX_ENV=integration mix test.integration
                ''')
            }

        }

        doStageUnlessRelease('Deploy to Dev') {
            scos.withDockerRegistry {
                image.push()
                image.push('latest')
            }
            deployReaperTo(environment: 'dev', tag: imageTag)
        }

        doStageIfPromoted('Deploy to Staging')  {
            def promotionTag = scos.releaseCandidateNumber()

            deployReaperTo(environment: 'staging', tag: imageTag)

            scos.applyAndPushGitHubTag(promotionTag)

            scos.withDockerRegistry {
                image.push(promotionTag)
            }
        }

        doStageIfRelease('Deploy to Production') {
            def releaseTag = env.BRANCH_NAME
            def promotionTag = 'prod'

            deployReaperTo(environment: 'prod', internal: false, tag: imageTag)

            scos.applyAndPushGitHubTag(promotionTag)

            scos.withDockerRegistry {
                image = scos.pullImageFromDockerRegistry("scos/reaper", env.GIT_COMMIT_HASH)
                image.push(releaseTag)
                image.push(promotionTag)
            }
        }
    }
}

def deployReaperTo(params = [:]) {
    def extraVars = [
      'image_tag': params.get('tag')
    ]
    def environment = params.get('environment')
    if (environment == null) throw new IllegalArgumentException("environment must be specified")

    def terraform = scos.terraform(environment)
    sh "terraform init && terraform workspace new ${environment}"
    terraform.plan(terraform.defaultVarFile, extraVars)
    terraform.apply()
}
