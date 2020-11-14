package com.shagnhaiuniversity.Azkaban;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Azkaban 上传的工作流文件只支持 xxx.zip 文件。 zip 应包含 xxx.job 运行作业所需的
 * 文件和任何文件（文件名后缀必须以.job 结尾，否则无法识别） 。作业名称在项目中必须是唯一的。
 * 可以通过的Azkaban的web界面截止上传任务的执行的需要的zip
 * 创建有依赖关系的多个 job 描述：
 * 1）多个job依次去使用的命令的相关的语句
 * 在服务器中使用的是的job 采用的是的vim的命令：
 * vim start.job
 * #start.job
 * type=command
 * command=touch /opt/module/kangkang.txt
 * vim step1.job
 * #step1.job
 * type=command
 * dependencies=start
 * command=echo "this is step1 job"
 * vim step2.job
 * #step2.job
 * type=command
 * dependencies=start
 * command=echo "this is step2 job"
 * vim finish.job
 * #finish.job
 * type=command
 * dependencies=step1,step2
 * command=echo "this is finish job"
 * <p>
 * 2) 将所有 job 资源文件打到一个 zip 包中
 * zip jobs.zip start.job step1.job step2.job  finish.job
 * <p>
 * 3)在 azkaban 的 web 管理界面创建工程并上传 zip 包
 * 这样的方式的是方便在测试的过程使用
 */
public class AzkabanTest {
    public static void main(String[] args) throws IOException {
        AzkabanTest azkabanTest = new AzkabanTest();
        azkabanTest.run();
    }

    public void run() throws IOException {
        // 根据需求编写具体代码
        String outpath = "";
        FileOutputStream fos = new FileOutputStream(outpath);
        fos.write("this is a java progress".getBytes());
        fos.close();
    }
}
