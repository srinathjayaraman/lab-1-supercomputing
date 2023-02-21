#!/bin/sh
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 1 -o brew-1
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 2 -o brew-2
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 4 -o brew-3
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 8 -o brew-4
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 16 -o brew-5
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 32 -o brew-6
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 64 -o brew-7
sudo docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit target/scala-2.12/lab-1_2.12-1.0.jar -p 128 -o brew-8
