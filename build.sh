version=`grep '<artifactId>kafka-connect-transform-common</artifactId>' -A1 -B1 pom.xml  | grep version | grep -o '[0-9]*\.[0-9]*\.[0-9]'`
docker push 003675007768.dkr.ecr.us-west-2.amazonaws.com/connect-packages:jcustenborder-kafka-connect-transform-common-${version}-`date +%y%m%d`-`git rev-parse --short HEAD`
echo docker push 003675007768.dkr.ecr.us-west-2.amazonaws.com/connect-packages:jcustenborder-kafka-connect-transform-common-${version}-`date +%y%m%d`-`git rev-parse --short HEAD`
