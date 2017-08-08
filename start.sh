export PATH=$PATH:/usr/local/bin 
if [ $# -ne 1 ]
then
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
else
    DIR=$1
fi

cd $DIR

# > /dev/null temporarily disables all error msgs
./stop.sh > /dev/null

if [ -f log/console.log ]
then
    rm log/console.log
fi

./node_modules/forever/bin/forever start -l $DIR/log/console.log --uid="dark_proteome" ./bin/www
