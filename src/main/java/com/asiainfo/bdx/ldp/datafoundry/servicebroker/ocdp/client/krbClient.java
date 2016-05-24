package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.ShellCommandUtil;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.krbConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEncoder;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java Client for manipulate MIT Kerberos. Including the following:
 * 1) Create krb principal
 * 2) Remove krb principal
 * 3) Generate krb keytab and keytab file
 *
 * @author whitebai1986@gmail.com
 *
 */
public class krbClient {
    //TODO: need to use java client to manipulate MIT kerberos instead of using kadmin CLI.

    private Logger logger = LoggerFactory.getLogger(krbClient.class);
    private static final Set<EncryptionType> DEFAULT_CIPHERS = Collections.unmodifiableSet(
            new HashSet<EncryptionType>() {{
                add(EncryptionType.DES_CBC_MD5);
                add(EncryptionType.DES3_CBC_SHA1_KD);
                add(EncryptionType.RC4_HMAC);
                add(EncryptionType.AES128_CTS_HMAC_SHA1_96);
                add(EncryptionType.AES256_CTS_HMAC_SHA1_96);
            }});
    private Set<EncryptionType> ciphers = new HashSet<EncryptionType>(DEFAULT_CIPHERS);
    private String userPrincipal;
    private String keytabLocation;
    private String adminPwd;
    private String kdcHost;
    private String realm;

    public krbClient(krbConfig krbConfig){
        this.userPrincipal = krbConfig.getUserPrincipal();
        this.keytabLocation = krbConfig.getKeytabLocation();
        this.adminPwd = krbConfig.getAdminPwd();
        this.kdcHost = krbConfig.getKdcHost();
        this.realm = krbConfig.getRealm();
    }

    /**
     * Invokes the kadmin shell command to issue queries
     *
     * @param query a String containing the query to send to the kdamin command
     * @return a ShellCommandUtil.Result containing the result of the operation
     * @throws KerberosKDCConnectionException       if a connection to the KDC cannot be made
     * @throws KerberosAdminAuthenticationException if the administrator credentials fail to authenticate
     * @throws KerberosRealmException               if the realm does not map to a KDC
     * @throws KerberosOperationException           if an unexpected error occurred
     */
    public ShellCommandUtil.Result invokeKAdmin(String query)
            throws KerberosOperationException {
        if (StringUtils.isEmpty(query)) {
            throw new KerberosOperationException("Missing kadmin query");
        }
        ShellCommandUtil.Result result;
        String defaultRealm = this.realm;

        List<String> command = new ArrayList<String>();

        String adminPrincipal = this.userPrincipal;

        String adminPassword = this.adminPwd;

        // Set the kdamin interface to be kadmin
        command.add("/usr/bin/kadmin");

        // Add explicit KDC admin host, if available
        String kdcHost = this.kdcHost;
        if (kdcHost != null) {
            command.add("-s");
            command.add(kdcHost);
        }

        // Add the administrative principal
        command.add("-p");
        command.add(adminPrincipal);

        if (adminPassword != null) {
            // Add password for administrative principal
            command.add("-w");
            command.add(adminPassword);
        }

        if (!StringUtils.isEmpty(defaultRealm)) {
            // Add default realm clause
            command.add("-r");
            command.add(defaultRealm);
        }

        // Add kadmin query
        command.add("-q");
        command.add(query);

        result = executeCommand(command.toArray(new String[command.size()]), null);

        if (!result.isSuccessful()) {
            // Test STDERR to see of any "expected" error conditions were encountered...
            String stdErr = result.getStderr();
            // Did admin credentials fail?
            if (stdErr.contains("Client not found in Kerberos database")) {
                throw new KerberosAdminAuthenticationException(stdErr);
            } else if (stdErr.contains("Incorrect password while initializing")) {
                throw new KerberosAdminAuthenticationException(stdErr);
            }
            // Did we fail to connect to the KDC?
            else if (stdErr.contains("Cannot contact any KDC")) {
                throw new KerberosKDCConnectionException(stdErr);
            } else if (stdErr.contains("Cannot resolve network address for admin server in requested realm while initializing kadmin interface")) {
                throw new KerberosKDCConnectionException(stdErr);
            }
            // Was the realm invalid?
            else if (stdErr.contains("Missing parameters in krb5.conf required for kadmin client")) {
                throw new KerberosRealmException(stdErr);
            } else if (stdErr.contains("Cannot find KDC for requested realm while initializing kadmin interface")) {
                throw new KerberosRealmException(stdErr);
            } else {
                throw new KerberosOperationException("Unexpected error condition executing the kadmin command");
            }
        }

        return result;
    }

    /**
     * Creates a new principal in a previously configured MIT KDC
     * <p/>
     * This implementation creates a query to send to the kadmin shell command and then interrogates
     * the result from STDOUT to determine if the operation executed successfully.
     *
     * @param principal a String containing the principal add
     * @param password  a String containing the password to use when creating the principal
     * @return an Integer declaring the generated key number
     * @throws KerberosOperationException           if an unexpected error occurred
     */
    public void createPrincipal(String principal, String password)
            throws KerberosOperationException{
        if (StringUtils.isEmpty(principal)) {
            throw new KerberosOperationException("Failed to create new principal - no principal specified");
        } else if (StringUtils.isEmpty(password)) {
            throw new KerberosOperationException("Failed to create new principal - no password specified");
        } else {
            // Create the kdamin query:  add_principal <-randkey|-pw <password>> [<options>] <principal>
            ShellCommandUtil.Result result = invokeKAdmin(String.format("add_principal -pw \"%s\" %s",
                    password, principal));

            // If there is data from STDOUT, see if the following string exists:
            //    Principal "<principal>" created
            String stdOut = result.getStdout();
            if((stdOut == null) || (! stdOut.contains(String.format("Principal \"%s\" created", principal)))){
                throw new KerberosOperationException(String.format("Failed to create service principal for %s\nSTDOUT: %s\nSTDERR: %s",
                        principal, stdOut, result.getStderr()));
            }
        }
    }

    /**
     * Removes an existing principal in a previously configured KDC
     * <p/>
     * The implementation is specific to a particular type of KDC.
     *
     * @param principal a String containing the principal to remove
     * @return true if the principal was successfully removed; otherwise false
     * @throws KerberosOperationException           if an unexpected error occurred
     */
    public boolean removePrincipal(String principal)
            throws KerberosOperationException{
        if (StringUtils.isEmpty(principal)) {
            throw new KerberosOperationException("Failed to remove new principal - no principal specified");
        } else {
            ShellCommandUtil.Result result = invokeKAdmin(String.format("delete_principal -force %s", principal));

            // If there is data from STDOUT, see if the following string exists:
            //    Principal "<principal>" created
            String stdOut = result.getStdout();
            return (stdOut != null) && !stdOut.contains("Principal does not exist");
        }
    }

    /**
     * Create a keytab using the specified principal and password.
     *
     * @param principal a String containing the principal to test
     * @param password  a String containing the password to use when creating the principal
     * @param keyNumber a Integer indicating the key number for the keytab entries
     * @return the created Keytab
     * @throws KerberosOperationException
     */
    public Keytab createKeyTab(String principal, String password, Integer keyNumber)
            throws KerberosOperationException {
        if (StringUtils.isEmpty(principal)) {
            throw new KerberosOperationException("Failed to create keytab file, missing principal");
        }

        if (password == null) {
            throw new KerberosOperationException(String.format("Failed to create keytab file for %s, missing password", principal));
        }

        List<KeytabEntry> keytabEntries = new ArrayList<KeytabEntry>();
        Keytab keytab = new Keytab();


        if (!ciphers.isEmpty()) {
            // Create a set of keys and relevant keytab entries
            Map<EncryptionType, EncryptionKey> keys = KerberosKeyFactory.getKerberosKeys(principal, password, ciphers);

            if (keys != null) {
                byte keyVersion = (keyNumber == null) ? 0 : keyNumber.byteValue();
                KerberosTime timestamp = new KerberosTime();

                for (EncryptionKey encryptionKey : keys.values()) {
                    keytabEntries.add(new KeytabEntry(principal, 1, timestamp, keyVersion, encryptionKey));
                }

                keytab.setEntries(keytabEntries);
            }
        }

        return keytab;
    }

    /**
     * Create a keytab file using the specified Keytab
     * <p/>
     * @param keytab                the Keytab containing the data to add to the keytab file
     * @param keytabFilePath a File containing the absolute path to where the keytab data is to be stored
     * @return true if the keytab file was successfully created; false otherwise
     */
    public boolean createKeyTabFile(Keytab keytab, String keytabFilePath)
            throws KerberosOperationException{
        if (keytabFilePath == null)
        {
            throw new KerberosOperationException("The destination file path is null.");
        }
        File keytabFile = new File(keytabFilePath);
        ensureKeytabFolderExists(keytabFilePath);
        try{
            keytab.write(keytabFile);
            return true;
        }catch (IOException e){
            throw new KerberosOperationException("Fail to export keytab file", e);
        }
    }

    /**
     * Create a keytab string using the base64 encode
     * <p/>
     * @param principal a String containing the principal to test
     * @param password  a String containing the password to use when creating the principal
     * @param keyNumber a Integer indicating the key number for the keytab entries
     * @return a keytab string using the base64 encode if keytab was successfully created; empty string otherwise
     */
    public  String createKeyTabString(String principal, String password, Integer keyNumber){
        String keyTabString = "";
        try{
            Keytab keytab = this.createKeyTab(principal, password, keyNumber);
            KeytabEncoder keytabEncoder = new KeytabEncoder();
            ByteBuffer keytabByteBuffer = keytabEncoder.write(keytab.getKeytabVersion(), keytab.getEntries());
            keyTabString = Base64.encodeBase64String(keytabByteBuffer.array());
        }catch (KerberosOperationException e){
            e.printStackTrace();
        }
        return keyTabString;
    }

    private void ensureKeytabFolderExists(String keytabFilePath) {
        String keytabFolderPath = keytabFilePath.substring(0, keytabFilePath.lastIndexOf("/"));
        File keytabFolder = new File(keytabFolderPath);
        if (!keytabFolder.exists() || !keytabFolder.isDirectory()) {
            keytabFolder.mkdir();
        }
    }

    protected ShellCommandUtil.Result executeCommand(String[] command, Map<String, String> envp)
            throws KerberosOperationException {

        if ((command == null) || (command.length == 0)) {
            return null;
        } else {
            try {
                return ShellCommandUtil.runCommand(command, envp);
            } catch (IOException e) {
                String message = String.format("Failed to execute the command: %s", e.getLocalizedMessage());
                logger.warn(message);
                throw new KerberosOperationException(message, e);
            } catch (InterruptedException e) {
                String message = String.format("Failed to wait for the command to complete: %s", e.getLocalizedMessage());
                logger.warn(message);
                throw new KerberosOperationException(message, e);
            }
        }
    }
}
