package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class ShellCommandUtil {

    /** Set to true when run on Linux platforms */
    public static final boolean LINUX
            = System.getProperty("os.name").startsWith("Linux");

    /**
     * Permission mask 777 allows everybody to read/modify/execute file
     */
    public static final String MASK_EVERYBODY_RWX = "777";

    /**
     * Gets file permissions on Linux systems.
     * Under Windows/Mac, command always returns MASK_EVERYBODY_RWX
     * @param path
     */
    public static String getUnixFilePermissions(String path) {
        String result = MASK_EVERYBODY_RWX;
        if (LINUX) {
            try {
                result = runCommand(new String[]{"stat", "-c", "%a", path}).getStdout();
            } catch (IOException e) {
                // Improbable
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();;
            }
        } else {
            System.out.println(String.format("Not performing stat -s \"%%a\" command on file %s " +
                    "because current OS is not Linux. Returning 777", path));
        }
        return result.trim();
    }

    /**
     * Sets file permissions to a given value on Linux systems.
     * On Windows/Mac, command is silently ignored
     * @param mode
     * @param path
     */
    public static void setUnixFilePermissions(String mode, String path) {
        if (LINUX) {
            try {
                runCommand(new String[]{"chmod", mode, path});
            } catch (IOException e) {
                // Improbable
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();;
            }
        } else {
            System.out.println(String.format("Not performing chmod %s command for file %s " +
                    "because current OS is not Linux ", mode, path));
        }
    }

    /**
     * Runs a command with a given set of environment variables
     * @param args a String[] of the command and its arguments
     * @param vars a Map of String,String setting an environment variable to run the command with
     * @return Result
     * @throws IOException
     * @throws InterruptedException
     */
    public static Result runCommand(String [] args, Map<String, String> vars) throws IOException,
            InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(args);

        if (vars != null) {
            Map<String, String> env = builder.environment();
            env.putAll(vars);
        }

        Process process = builder.start();
        // if command output is too intensive
        process.waitFor();
        String stdout = streamToString(process.getInputStream());
        String stderr = streamToString(process.getErrorStream());
        int exitCode = process.exitValue();
        return new Result(exitCode, stdout, stderr);
    }

    /**
     * Run a command
     * @param args A String[] of the command and its arguments
     * @return Result
     * @throws IOException
     * @throws InterruptedException
     */
    public static Result runCommand(String [] args) throws IOException,
            InterruptedException {
        return runCommand(args, null);
    }

    private static String streamToString(InputStream is) throws IOException {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader reader = new BufferedReader(isr);
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public static class Result {

        Result(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        private final int exitCode;
        private final String stdout;
        private final String stderr;

        public int getExitCode() {
            return exitCode;
        }

        public String getStdout() {
            return stdout;
        }

        public String getStderr() {
            return stderr;
        }

        public boolean isSuccessful() {
            return exitCode == 0;
        }
    }
}
