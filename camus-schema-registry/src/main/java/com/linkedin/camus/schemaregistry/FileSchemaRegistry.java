package com.linkedin.camus.schemaregistry;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Properties;


/**
 * Not thread safe.
 * 
 * @param <S>
 *            The type of the schema that this registry manages.
 */
public class FileSchemaRegistry<S> implements SchemaRegistry<S> {
  private final File root;
  private final Serde<S> serde;

  public void init(Properties props) {
  }

  public FileSchemaRegistry(File root, Serde<S> serde) {
    this.root = root;
    this.serde = serde;
  }

  @Override
  public String register(String topic, S schema) {
    FileOutputStream out = null;

    File topicDir = getTopicPath(topic);

    if (!topicDir.exists()) {
      topicDir.mkdirs();
    }

    try {
      byte[] bytes = serde.toBytes(schema);
      String id = SHAsum(bytes);
      File file = getSchemaPath(topic, id, true);
      out = new FileOutputStream(file);
      out.write(bytes);

      // move any old "latest" files to be regular schema files
      for (File fileToRename : topicDir.listFiles()) {
        if (!fileToRename.equals(file) && fileToRename.getName().endsWith(".latest")) {
          String oldName = fileToRename.getName();
          // 7 = len(.latest)
          File renameTo = new File(fileToRename.getParentFile(), oldName.substring(0, oldName.length() - 7));
          fileToRename.renameTo(renameTo);
        }
      }
      return id;
    } catch (Exception e) {
      throw new SchemaRegistryException(e);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          throw new SchemaRegistryException(e);
        }
      }
    }
  }

  @Override
  public S getSchemaByID(String topic, String id) {
    File file = getSchemaPath(topic, id, false);

    if (!file.exists()) {
      file = getSchemaPath(topic, id, true);
    }

    if (!file.exists()) {
      throw new SchemaNotFoundException("No matching schema found for topic " + topic + " and id " + id + ".");
    }

    return serde.fromBytes(readBytes(file));
  }

  @Override
  public SchemaDetails<S> getLatestSchemaByTopic(String topicName) {
    File topicDir = getTopicPath(topicName);
    if (topicDir.exists()) {
      for (File file : topicDir.listFiles()) {
        if (file.getName().endsWith(".latest")) {
          String id = file.getName().replace(".schema.latest", "");
          return new SchemaDetails<S>(topicName, id, serde.fromBytes(readBytes(file)));
        }
      }
    }
    throw new SchemaNotFoundException("Unable to find a latest schema for topic " + topicName + ".");
  }

  private byte[] readBytes(File file) {
    DataInputStream dis = null;
    byte[] bytes = new byte[(int) file.length()];

    try {
      dis = new DataInputStream(new FileInputStream(file));
      dis.readFully(bytes);
    } catch (Exception e) {
      throw new SchemaRegistryException(e);
    } finally {
      if (dis != null) {
        try {
          dis.close();
        } catch (Exception e) {
          throw new SchemaRegistryException(e);
        }
      }
    }
    return bytes;
  }

  private File getTopicPath(String topic) {
    return new File(root, topic);
  }

  private File getSchemaPath(String topic, String id, boolean latest) {
    return new File(getTopicPath(topic), id + ".schema" + ((latest) ? ".latest" : ""));
  }

  public static String SHAsum(byte[] convertme) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    return byteArray2Hex(md.digest(convertme));
  }

  private static String byteArray2Hex(final byte[] hash) {
    Formatter formatter = new Formatter();
    for (byte b : hash) {
      formatter.format("%02x", b);
    }
    String hex = formatter.toString();
    formatter.close();
    return hex;
  }
}
