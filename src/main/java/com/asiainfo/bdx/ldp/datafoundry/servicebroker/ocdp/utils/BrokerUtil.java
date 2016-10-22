package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.support.LdapNameBuilder;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;
import java.util.UUID;
import java.io.IOException;

/**
 * Created by baikai on 10/17/16.
 */
public class BrokerUtil {

    private final static int uidNumberBase = 1100;

    public static void authentication(Configuration conf, String userPrincipal, String keyTabFilePath){
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(userPrincipal, keyTabFilePath);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static String generateAccountName(){
        String uuid = UUID.randomUUID().toString();
        return uuid.substring(0,18);
    }

    public static void createLDAPUser(LdapTemplate ldapTemplate, String accountName, String groupName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        Attributes userAttributes = new BasicAttributes();
        userAttributes.put("memberOf", "cn=" + groupName +",ou=Group,dc=asiainfo,dc=com");
        BasicAttribute classAttribute = new BasicAttribute("objectClass");
        classAttribute.add("account");
        classAttribute.add("posixAccount");
        userAttributes.put(classAttribute);
        userAttributes.put("cn", accountName);
        userAttributes.put("uidNumber", "1110");
        userAttributes.put("gidNumber", "1110");
        userAttributes.put("homeDirectory", "/home/" + accountName);
        ldapTemplate.bind(ldapName, null, userAttributes);
    }

    public static void removeLDAPUser(LdapTemplate ldapTemplate, String accountName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        ldapTemplate.unbind(ldapName);
    }

}
