import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

//This class is used to execute a job by creating a new local process after its files(jar and input) are transferred from the master.
public class ProcessThread implements Runnable {
    
    private EndJob job;
    private Task task;
    private String jobpath;
    private String jarpath;
    private String inputpath;
    private String inputformat;
    private long timelimit;
    
    public ProcessThread(EndJob job, Task task, long timelimit)
    throws IOException, InterruptedException {
        this.job = job;
        this.task = task;
        this.timelimit = timelimit;
        jobpath = job.getPath();
        jarpath = job.getJarPath();
        inputpath = job.getInputPath();
        // create output file according to the format of the input one.
        inputformat = inputpath.substring(inputpath.lastIndexOf("."),
                                          inputpath.length());
    }
    
    // This method is used when process of jar file is successful. It deletes
    // the standard output file and error file. Set job status to "Finished" and
    // add output to the task list.
    private void success(File stdout, File stderr, String outputpath) {
        if (stdout.exists()) {
            stdout.delete();
        }
        if (stderr.exists()) {
            stderr.delete();
        }
        job.setStatus(1);
        job.setOutputPath(outputpath);
        task.putJob(job);
    }
    
    // This method is used when process of jar file is failed. It deletes the
    // output file. Set the paths of standard output file and error file to the
    // job. Add output to the task list.
    private void fail(File output, String stdoutputpath, String stderrpath) {
        System.out.println("The status of process is: Failed");
        if (output.exists()) {
            output.delete();
        }
        job.setStatus(2);
        job.setStdOutputPath(stdoutputpath);
        job.setStdErrPath(stderrpath);
        task.putJob(job);
    }
    
    public void run() {
        
        // Create two new files to store output and errorInfo.
        File jarFile = new File(jarpath);
        File inputFile = new File(inputpath);
        String jarFileName = jarFile.getName().substring(0,
                                                         jarFile.getName().lastIndexOf("."));
        String inputFileName = inputFile.getName().substring(0,
                                                             inputFile.getName().lastIndexOf("."));
        String commonPath = jobpath + File.separator + jarFileName + "_"
        + inputFileName;
        String stdoutputpath = commonPath + "_stdout.txt";
        String stderrpath = commonPath + "_stderr.txt";
        String outputpath = commonPath + "_output" + inputformat;
        
        File stdout = new File(stdoutputpath);
        File output = new File(outputpath);
        File stderr = new File(stderrpath);
        
        try {
            stdout.createNewFile();
            stderr.createNewFile();
            output.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // execute the jar command.
        String javaexepath = Paths
        .get(System.getProperty("java.home"), "bin", "java")
        .toAbsolutePath().toString();
        System.out.println("start to run the jar file.");
        
        ProcessBuilder workerprocessbuilder = new ProcessBuilder(javaexepath,
                                                                 "-jar", jarpath, inputpath, output.getAbsolutePath());
        
        // redirect them to the file created before.
        workerprocessbuilder.redirectInput();
        workerprocessbuilder.redirectErrorStream(false);
        workerprocessbuilder.redirectError(stderr);
        workerprocessbuilder.redirectOutput(stdout);
        
        Process workerprocess;
        try {
            workerprocess = workerprocessbuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        System.out.println("The status of process is: Running");
        
        // set timelimit to specific value if user specify them.
        if (timelimit > 0) {
            try {
                if (workerprocess.waitFor(timelimit, TimeUnit.SECONDS)) {
                    if (workerprocess.exitValue() == 0) {
                        System.out
                        .println("The status of process is: Finished");
                        
                        success(stdout, stderr, outputpath);
                        
                    } else {
                        fail(output, stdoutputpath, stderrpath);
                    }
                } else {
                    workerprocess.destroy();
                    System.out.println("The status of process is: Failed");
                    FileOutputStream fos = new FileOutputStream(stderr);
                    PrintStream ps = new PrintStream(fos);
                    PrintStream original = System.out;
                    System.setOut(ps);
                    System.setOut(original);
                    System.out
                    .println("Error: Job stops executing because it has exceeded the "
                             + "limited time. Which is "
                             + timelimit
                             + " seconds.");
                    
                    fail(output, stdoutputpath, stderrpath);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // Run until it finishes if user doesn't specify the limitTime.
        else {
            try {
                workerprocess.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (workerprocess.exitValue() == 0) {
                System.out.println("The status of process is: Finished");
                success(stdout, stderr, outputpath);
            } else {
                
                fail(output, stdoutputpath, stderrpath);
            }
        }
    }
}
