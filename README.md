整体思路
根据作业要求需要

统计 Backblaze 每天 Drive count/Drive failures
统计 Backblaze 每年 Drive failures count by brand
upload：从 Backblaze 直接解压zip然后上传到aws的s3bucket
将远程的 zip 文件下载解压到 s3://47-data-test/data目录下。
待改进
数据量太大了，需要自己手动的在 upload_file_to_s3 中设置某个季度的下载链接,当然也可以用数组去把所有的下载链接放上去，但这样的话会耗时很久。

Process：数据分析（daily和annual的计算）
这块分为了两个scripy，分别是 daily_data_analysis.py 和 annual_data_analysis.py.
将 s3 中data目录下的 csv 文件读取进行计算，最后得到daily和annul的csv结果，以s3a://47-data-test/daily-analysis.csv，s3a://47-data-test/annual-analysis.csv形式存储在s3中
待改进
1. 这两个脚本直接运行诗会报错的，需要 运行spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4  脚本名称  这个命令添加hadoop-aws依赖才可以运行成功，应该是可以直接在程序
配置这个依赖，但我目前还没有找到正确的配置方法，这是可以优化的地方
2. 只是处理一个季度的数据，运行脚本都需要耗时50分钟，时间花费太大了

Publish:数据发布
这里我就是在数据分析后，直接将结果转成csv文件上传到了s3上


总体待改进： 目前上述脚本都只能在local环境上成功执行并得到对应结果，但是一旦放到emr集群上，脚本都会失败，需要花时间去探索直接在emr上运行脚本。可以用一个main函数去分别执行upload,process,publish对应的函数，这样
流程会更加的清晰，这个我会慢慢修改结构
