背景：在做视频相似度识别项目的时候，我们需要计算图片指纹的汉明距离。刚开始我们是直接用scala来计算汉明距离，性能略让人失望，后来经人点拨，c语言的性能会好很多，于是就倒腾用scala调用c库函数来计算，经验证，在我们的场景下，用scala通过jni调用c，比直接用scala计算，性能上要快十多倍。在探索在spark分布式环境下用scala通过jni调用c的路上，也经过了大半天多时间的摸索，才用scala调用c正常运行。所以整理一下scala直接调用jni方法，一来便于自己记忆，二来分享给需要的童鞋们

JNI (Java Native Interface,Java本地接口)是一种编程框架,使得Java虚拟机中的Java程序可以调用本地应用/或库,也可以被其他程序调用。 本地程序一般是用其它语言（C、C++或汇编语言等）编写的, 并且被编译为基于本机硬件和操作系统的程序。其余介绍性的不多说，大家可以Google，下面从用一个demo从实践角度上详细讲讲怎么用好了。

1. 先创建一个项目，取名叫testsherry，代码文件主要有三个，HammingDistJNI.c是汉明距离计算的c代码，HammingDistJNI.scala是调用c代码的scala接口，HammingDist.scala是主程序代码，里面有两种使spark动态加载so文件的方法。



2. HammingDistJNI.scala编译

package com.tencent.omg.distance

class HammingDistJNI {
  @native def getHammingDist(hashA: String, hashB: String): Int
}
 在~/testsherry/src/main/c++目录下，运行（scala目录换成你自己的目录即可）

 ~/scala/bin/scalac ../scala/com/tencent/omg/distance/HammingDistJNI.scala
3. 生成HammingDistJNI.h头文件

SCALA_HOME=~/scala       #改成你自己的scala目录
SCALA_LIB=$SCALA_HOME/lib
SCALA_CP=$SCALA_LIB/scala-library.jar:$SCALA_LIB/scala-reflect.jar
javah -cp $SCALA_CP:. com.tencent.omg.distance.HammingDistJNI
运行之后，就会在 ~/testsherry/src/main/c++目录下，生成com_tencent_omg_distance_HammingDistJNI.h的头文件，可以打开来看看具体内容

#include <jni.h>
/* Header for class com_tencent_omg_distance_HammingDistJNI */

#ifndef _Included_com_tencent_omg_distance_HammingDistJNI
#define _Included_com_tencent_omg_distance_HammingDistJNI
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_tencent_omg_distance_HammingDistJNI
 * Method:    getHammingDist
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_tencent_omg_distance_HammingDistJNI_getHammingDist
  (JNIEnv *, jobject, jstring, jstring);

#ifdef __cplusplus
}
#endif
#endif
 这个是自动生成的，如果你中间代码参数改来改去，package名字变来变去，就得注意重新生成这个头文件了，要不然会报找不到这个头文件的错误

4. libHammingDistJNI.c文件编写

#include "com_tencent_omg_distance_HammingDistJNI.h"
#include <stdlib.h>
JNIEXPORT jint JNICALL Java_com_tencent_omg_distance_HammingDistJNI_getHammingDist
  (JNIEnv *env, jobject obj, jstring jhashA, jstring jhashB)
{
    const char *c_hashA = NULL;
    const char *c_hashB = NULL;
	c_hashA = (*env)->GetStringUTFChars(env, jhashA, NULL);
	c_hashB = (*env)->GetStringUTFChars(env, jhashB, NULL);

    int hamming = 0;
    if(c_hashA == NULL || c_hashB == NULL)
    {
        hamming = -1;
    }
    else{
        unsigned long ul_hashA = strtoul(c_hashA, NULL, 0);
    	unsigned long ul_hashB = strtoul(c_hashB, NULL, 0);

    	unsigned long ans = (ul_hashA ^ ul_hashB);
        while(ans > 0)
        {
            ans = ans & (ans - 1);
            hamming += 1;
        }
    }

    (*env)->ReleaseStringUTFChars(env, jhashA, c_hashA);
    (*env)->ReleaseStringUTFChars(env, jhashB, c_hashB);

	return hamming;
}
 这块是汉明距离计算的c代码，这块需要注意两个地方：

        1. Java /Scala中的字符串(String) 也是对象, 有 length 属性,并且是编码过的. 读取或者创建字符串都需要一次时间复杂度为 O(n) 的复制操作，所以最好不要用字符串来承载参数，比如我们这个汉明距离计算也是的，如果数字特别大，JNI里有没有bigint这种类型，那就可以把数拆分成两个long来处理，实验证明，会比字符串快很多（demo里只是用string举个例子，真是算汉明距离，还是得转成long来处理，高效很多）

        2. 如果在c里面使用了字符串或者其他指针，确保最后都释放了。如果从中间直接返回，漏了释放指针，在spark上跑的时候，就会报各种内存空间不足。

        3. 当然汉明距离计算，还有很多做法，这块我们也尝试了很多，有机会可以单独开一篇来整理一下。

5. libHammingDistJNI.so文件的生成

gcc -O2 -shared -fPIC -I/usr/include -I$JAVA_HOME/include -I$JAVA_HOME/include/linux HammingDistJNI.c -o libHammingDistJNI.so
注意两个地方：

        1. 要把$JAVA_HOME/include/linux加上，否则会找不到jni.h文件

         2. so文件命名记得加上lib，也就是 libHammingDistJNI.so，不要写成HammingDistJNI.so，因为c++的so文件都是以lib开头，也会按照这个规律去找这个so文件的


6.在spark环境中动态加载.so文件

spark是分布式集群，所以我们需要每个executor去load各自的 HammingDistJNI.so文件，然后再在自己的partition上进行调用HammingDistJNI.getHammingDist来计算汉明距离。对于单机程序而言，在运行jar包的时候，设置一下java.library.path就可以让jvm找到对应so文件的位置，从而load so文件。但是，在spark的executor节点去加载.so库的时候，JVM已经启动了，java.library.path也已经固定了，executor按着java.library.path找不到我们的HammingDistJNI.so库，会报链接文件不存在。所以下面有两种办法可以解决：

方法一：

通过spark-submit的--files参数将接入机上的HammingDistJNI.so文件上传到spark集群上，然后通过SparkFiles.getRootDirectory的方法拿到上传文件的根目录，再将这个目录动态的添加到java.library.path中，具体代码如下：

  def loadResource1(): Unit ={
    val rootDir= SparkFiles.getRootDirectory()
  
    try{
      val field = classOf[ClassLoader].getDeclaredField("usr_paths")
      field.setAccessible(true)
      val paths = field.get(null).asInstanceOf[Array[String]]
      if(!(paths contains rootDir)){
        field.set(null, paths :+ rootDir)
        System.setProperty("java.library.path", System.getProperty("java.library.path") +
                                                                    java.io.File.pathSeparator +
                                                                    rootDir)
      }
    }
    catch{
      case _: IllegalAccessException =>
        sys.error("Insufficient permissions; can't modify private variables.")
      case _: NoSuchFieldException =>
        sys.error("JVM implementation incompatible with path hack")
    }
    System.loadLibrary("HammingDistJNI")
  }
 注意了是System.loadLibrary("HammingDistJNI")，不要把.so也写进去了

 方法二：将.so文件作为资源文件打包进jar包，jar包会广播给所有的executor节点，然后通过读取资源文件的方式将.so文件读到内存中，然后重新写到机器的java.io.tmpdir目录下，需保证这个目录可写，所以用java.io.tmpdir肯定是可以的，你也可以换成其他可写目录。具体代码如下

def loadResource2(): Unit ={
    val inputStream = this.getClass.getClassLoader.getResource("resource/libHammingDistJNI.so").openStream
    val soFile = new File(System.getProperty("java.io.tmpdir"), "libHammingDistJNI.so")
    
    if (!soFile.exists) {
      val outputStream = new FileOutputStream(soFile)
      val array = new Array[Byte](8192)
      var i = inputStream.read(array)
      while ( {
        i != -1
      }) {
        outputStream.write(array, 0, i)
      
        i = inputStream.read(array)
      }
      outputStream.close()
    }
    System.load(soFile.getAbsolutePath)
  }
  
将libHammingDistJNI.so文件作为资源文件打包到jar，也比较简单：1. 将libHammingDistJNI.so放到src/main/resources/下； 2. 修改pom.xml文件，指定一下资源文件打到jar后的路径resource，这个路径也在上面的代码里有所体现，是一个对应关系

<build>   
     <resources>
            <resource>
                <directory>src/main/resources</directory>
                <targetPath>resource</targetPath>
            </resource>
        </resources>
    </build>
 打包之后，可以运行一下命令 

jar tvf target/testsherry-jar-with-dependencies.jar 
 可以看到



我们的HammingDistJNI.so文件确实打包进来了

7. HammingDist.scala文件编写

综合了上面两个loadResource的方法，以及不能频繁去实例化加载.so文件，我们使用了单例模式，使得每个executor节点只需load一次.so文件，同时在具体任务执行的时候，用mappartition代替map，使得每个new一个HammingDist对象即可。

package com.tencent.omg.distance

import java.io.File
import java.io.FileOutputStream
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.sql.SparkSession
class HammingDist {
  @transient  private var hammingInst: HammingDistJNI = _
  private def getInstance(): HammingDistJNI = {
    if (hammingInst == null) {
      loadResource2()
      hammingInst = new HammingDistJNI
    }
    hammingInst
  }
  
  def loadResource1(): Unit ={
    val rootDir= SparkFiles.getRootDirectory()
  
    try{
      val field = classOf[ClassLoader].getDeclaredField("usr_paths")
      field.setAccessible(true)
      val paths = field.get(null).asInstanceOf[Array[String]]
      if(!(paths contains rootDir)){
        field.set(null, paths :+ rootDir)
        System.setProperty("java.library.path", System.getProperty("java.library.path") +
                                                                    java.io.File.pathSeparator +
                                                                    rootDir)
      }
    }
    catch{
      case _: IllegalAccessException =>
        sys.error("Insufficient permissions; can't modify private variables.")
      case _: NoSuchFieldException =>
        sys.error("JVM implementation incompatible with path hack")
    }
    System.loadLibrary("HammingDistJNI")
  }
  
  def loadResource2(): Unit ={
    val inputStream = this.getClass.getClassLoader.getResource("resource/libHammingDistJNI.so").openStream
    val soFile = new File(System.getProperty("java.io.tmpdir"), "libHammingDistJNI.so")
    
    if (!soFile.exists) {
      val outputStream = new FileOutputStream(soFile)
      val array = new Array[Byte](8192)
      var i = inputStream.read(array)
      while ( {
        i != -1
      }) {
        outputStream.write(array, 0, i)
      
        i = inputStream.read(array)
      }
      outputStream.close()
    }
    System.load(soFile.getAbsolutePath)
  }
  
  def hammingDist(hashA : String, hashB : String) : Int = {
    val hammingJni = getInstance()
    val hamming = hammingJni.getHammingDist(hashA, hashB)
    hamming
  }
}

object HammingDist{
  def main(args:Array[String]) {
    var appName = "HammingDist"
    val conf = new SparkConf().
        setAppName(appName)
  
    val spark = SparkSession.
        builder().
        appName(appName).
        config(conf).
        getOrCreate()
  
    val str="""5099785081995195,16710456112897396495,1982488167573249,9764860350098042879,84658075012895,4718913988321036,18437692359328333824,18441868972816105506,17852763487973344258,18438515930120076959,18446710958990561280,18228866988188516416,14992500781213351166,17093869011967,2234629857369587711,18446673185400160320,72586325532278719,1133789313024409345,264969417358848,15762703066857471,18444381332768342465,9695957760534972175,6989587832000995327,18374401267562190848,18374401267562190848,230800579171124738,1143667022617934863,4575656122035259338,16709479143112378255,506381210876372967,16059203921023067107,59635053499453184,18446744039349878976,18446744039349878976,18446604095894327527,212192842022140,212192842022140,18436032191621300673,18446744042302603264,18446744042302603264,18429789583085931549,18445618175962578928,18446464806362879999,18377224682878040096,16271748223566727678,14543182056671737689,18446735222051442688,18446681999503524099,16700334346125393446,18311370039967152176,18417976455768689664,18385927813539627055,18385927813539627055,18439952237921846286,18230281986910652175,1082841962108809207,1082841962108809207,1082841962108809207,1082841962108809207,9187492775034615807,72056628191682560,15769061634734754330,9288463114566631933,18446611099568766976,18446611099568766976,18446611099568766976,18446611099568766976,18439878291889915904,16210988199491796608,17577266619955052672,18446743952376725506,17835243634865973504,5964235964153644,6919533708881544131,18439649885318808579,9236723309444863,18444461363033014522,9766055791399272450,18302334185930522624,18446470325496907524,18407182816564546567,18441394419321324676,18446717425769644033,16123167040935567377,18446730827497734144,18446730827497734144,18446730827497734144,18446730827497734144,18446730827497734144,18446657281420493699,67537471653724417,273214935350527,6985189058191834943,13839623712841465080,13838998723159392190,17933558671816128885,11042050573310165386,11042050573310165386,11042050573310165386,12718233786664413183"""
    val hashArr = str.split(",")
    val rdd = spark.sparkContext.parallelize(hashArr)
    val hammingRdd = rdd.mapPartitions(itr => {
      val hammingInst = new HammingDist
      itr.map(row => (row, hammingInst.hammingDist(row, "16710456112897396495")))
    })
    hammingRdd.take(2).foreach(println)
  }
}
我们这个比较简单，c里面只是进行了计算，并没有涉及到数据库等连接，所以没有做连接池等，如果你的c代码里面涉及到了资源连接等，那这个scala类还需要考虑资源池的建立释放等。

总结而言：

这两种loadResource方式都可以，对于loadResource1而言，需要在任务提交脚本上把.so文件带上，然后在代码里将代码目录加到java.library.path路径下，然后loadLibrary即可。运行命令如下：

#!/bin/bash
 spark-submit --master yarn \
 --deploy-mode client \
 --driver-memory 4g \
 --executor-memory 1g \
 --executor-cores 3 \
 --num-executors  2\
 --files ../testsherry/src/main/resources/libHammingDistJNI.so \
 --class com.tencent.omg.distance.HammingDist \
 ../testsherry/target/testsherry-jar-with-dependencies.jar
 而对于loadResource2方式而言，就不需要把.so文件在--files参数里带上，但是需要在pom.xml文件加上对资源文件的打包配置，然后读资源文件，写到一个临时文件目录下，然后load即可。

