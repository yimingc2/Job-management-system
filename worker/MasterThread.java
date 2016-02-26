import java.io.*;
import java.net.*;
import java.util.Base64;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MasterThread implements Runnable {
    
    private int id;
    private Socket sock;
    private String masterPath;
    private Task task;
    private ProcessNum process;
    
    public MasterThread(ProcessNum process, String mainPath, Socket sock, int id) {
        this.process = process;
        this.sock = sock;
        this.id = id;
        task = new Task();
        masterPath = mainPath + File.separator + "Master" + id;
        File file = new File(masterPath);
        file.mkdir();
    }
    
    public void run() {
        try {
            InputStream sockin = sock.getInputStream();
            InputStreamReader inputstreamReader = new InputStreamReader(sockin);
            BufferedReader bufferedReader = new BufferedReader(
                                                               inputstreamReader);
            
            OutputStream sockout = sock.getOutputStream();
            OutputStreamWriter outputstreamWriter = new OutputStreamWriter(
                                                                           sockout);
            BufferedWriter bufferedWriter = new BufferedWriter(
                                                               outputstreamWriter);
            
            while (true) {
                // Receive new job from a master and send its status
                // and number of processes to that master.
                while (true) {
                    String checkJob = bufferedReader.readLine();
                    JSONObject obj = new JSONObject();
                    obj = (JSONObject) JSONValue.parse(checkJob);
                    if (obj.get("command").equals("have job")) {
                        sendStatus(task, bufferedWriter);
                        sendProcessNumber(bufferedWriter);
                        receiveJob(bufferedReader, bufferedWriter);
                    } else {
                        sendProcessNumber(bufferedWriter);
                        break;
                    }
                }
                // Check if there is finished job and send its status and number
                // of processes to that master
                while (true) {
                    if (task.existNewJob()) {
                        JSONObject obj = new JSONObject();
                        obj.put("command", "have output");
                        bufferedWriter.write(obj.toJSONString() + "\n");
                        bufferedWriter.flush();
                        
                        sendStatus(task, bufferedWriter);
                        sendProcessNumber(bufferedWriter);
                        
                        sendJob(bufferedReader, bufferedWriter);
                    } else {
                        JSONObject obj = new JSONObject();
                        obj.put("command", "no output");
                        bufferedWriter.write(obj.toJSONString() + "\n");
                        bufferedWriter.flush();
                        sendProcessNumber(bufferedWriter);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Send the number of processes a worker executes
    private void sendProcessNumber(BufferedWriter bufferedWriter)
    throws IOException, InterruptedException {
        Thread.sleep(100);
        JSONObject obj = new JSONObject();
        obj.put("processnum", process.countProcess());
        bufferedWriter.write(obj.toJSONString() + "\n");
        bufferedWriter.flush();
    }
    
    private void receiveJob(BufferedReader bufferedReader,
                            BufferedWriter bufferedWriter) throws Exception, IOException {
        String jarPath = null;
        String inputPath = null;
        
        JSONObject obj = new JSONObject();
        String jobNameAndTime = bufferedReader.readLine();
        
        obj = (JSONObject) JSONValue.parse(jobNameAndTime);
        String jobName = (String) obj.get("jobname");
        String timeLimit0 = (String) obj.get("time");
        long timeLimit = Long.parseLong(timeLimit0);
        String jobPath = masterPath + File.separator + jobName;
        File fileFolder = new File(jobPath);
        fileFolder.mkdir();
        
        int i = 0;
        while (i < 2) {
            File file = createFile(bufferedReader, bufferedWriter, jobPath);
            String filePath = file.getAbsolutePath();
            if (i == 0) {
                jarPath = filePath;
            } else {
                inputPath = filePath;
            }
            receiveFile(bufferedReader, filePath);
            
            JSONObject obj2 = new JSONObject();
            obj2.put("command", "finish");
            bufferedWriter.write(obj2.toJSONString() + "\n");
            bufferedWriter.flush();
            
            i++;
        }
        EndJob job = new EndJob(jobName, jobPath, jarPath, inputPath);
        process.addProcess();
        
        Thread t = new Thread(new ProcessThread(job, task, timeLimit));
        t.start();
        
        JSONObject obj1 = new JSONObject();
        obj1.put("status", "start");
        bufferedWriter.write(obj1.toJSONString() + "\n");
        bufferedWriter.flush();
    }
    
    private void receiveFile(BufferedReader bufferedReader, String filePath)
    throws IOException, FileNotFoundException {
        JSONObject obj1 = new JSONObject();
        String big0 = bufferedReader.readLine();
        obj1 = (JSONObject) JSONValue.parse(big0);
        String big = (String) obj1.get("size");
        int size = Integer.parseInt(big);
        FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        int sum = 0;
        DataInputStream in = new DataInputStream(sock.getInputStream());
        if (size != 0) {
            while (true) {
                String dataJason = in.readUTF();
                JSONObject obj = new JSONObject();
                obj = (JSONObject) JSONValue.parse(dataJason);
                String fileData = (String) obj.get("filedata");
                byte[] asBytes = Base64.getDecoder().decode(fileData);
                sum = sum + asBytes.length;
                fileOutputStream.write(asBytes);
                if (sum == size) {
                    break;
                }
            }
        }
        fileOutputStream.close();
    }
    
    private File createFile(BufferedReader bufferedReader,
                            BufferedWriter bufferedWriter, String jobPath) throws Exception {
        JSONObject obj = new JSONObject();
        String filename0 = bufferedReader.readLine();
        obj = (JSONObject) JSONValue.parse(filename0);
        String fileName = (String) obj.get("filename");
        File file = new File(jobPath + File.separator + fileName);
        file.createNewFile();
        
        JSONObject obj1 = new JSONObject();
        obj1.put("command", "ready");
        bufferedWriter.write(obj1.toJSONString() + "\n");
        bufferedWriter.flush();
        return file;
    }
    
    private void sendJob(BufferedReader bufferedReader,
                         BufferedWriter bufferedWriter) throws IOException {
        EndJob tempJob = task.getNewJob();
        JSONObject obj = new JSONObject();
        obj.put("jobid", tempJob.getId());
        String status = tempJob.getStatus();
        if (status.equals("finished")) {
            obj.put("filenum", "1");
            bufferedWriter.write(obj.toJSONString() + "\n");
            bufferedWriter.flush();
            sendFile(tempJob.getOutputPath(), bufferedReader, bufferedWriter);
        } else {
            obj.put("filenum", "2");
            bufferedWriter.write(obj.toJSONString() + "\n");
            bufferedWriter.flush();
            sendFile(tempJob.getStdOutputPath(), bufferedReader, bufferedWriter);
            sendFile(tempJob.getStdErrPath(), bufferedReader, bufferedWriter);
        }
    }
    
    private void sendFile(String outputPath, BufferedReader bufferedReader,
                          BufferedWriter bufferedWriter) {
        try {
            File outputFile = new File(outputPath);
            String fileName = outputFile.getName();
            int size = 0;
            FileInputStream fileInputStream = new FileInputStream(outputFile);
            size = fileInputStream.available();
            JSONObject obj = new JSONObject();
            obj.put("filename", fileName);
            obj.put("size", String.valueOf(size));
            bufferedWriter.write(obj.toJSONString() + "\n");
            bufferedWriter.flush();
            
            JSONObject obj1 = new JSONObject();
            String info0 = bufferedReader.readLine();
            obj1 = (JSONObject) JSONValue.parse(info0);
            String info = (String) obj1.get("command");
            
            OutputStream sockout = sock.getOutputStream();
            
            if (info.equals("ready")) {
                byte[] buffer = new byte[1024];
                int len = 0;
                DataOutputStream out = new DataOutputStream(
                                                            sock.getOutputStream());
                while (true) {
                    len = fileInputStream.read(buffer);
                    if (len != -1) {
                        byte[] buffer2 = new byte[len];
                        for (int i = 0; i < len; i++) {
                            buffer2[i] = buffer[i];
                        }
                        String sendout = Base64.getEncoder().encodeToString(
                                                                            buffer2);
                        JSONObject dataJason = new JSONObject();
                        dataJason.put("filedata", sendout);
                        out.writeUTF(dataJason.toJSONString());
                        out.flush();
                    } else {
                        break;
                    }
                }
                bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void sendStatus(Task task, BufferedWriter bufferedWriter)
    throws IOException {
        while (true) {
            if (task.existNewStatus()) {
                process.removeProcess();
                JSONObject obj = new JSONObject();
                obj.put("command", "has status");
                bufferedWriter.write(obj.toJSONString() + "\n");
                bufferedWriter.flush();
                EndJob job = task.peekNewJob();
                JSONObject obj1 = new JSONObject();
                obj1.put("jobid", job.getId());
                obj1.put("status", job.getStatus());
                bufferedWriter.write(obj1.toJSONString() + "\n");
                bufferedWriter.flush();
            } else {
                JSONObject obj = new JSONObject();
                obj.put("command", "no status");
                bufferedWriter.write(obj.toJSONString() + "\n");
                bufferedWriter.flush();
                break;
            }
        }
    }
}
