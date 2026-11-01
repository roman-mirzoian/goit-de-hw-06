package local.windows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Local file system for running Spark Structured Streaming on Windows without
 * winutils.exe and hadoop.dll.
 *
 * Hadoop sets POSIX permissions on every directory it creates (for example the streaming
 * checkpoint folder) through winutils.exe and checks read access through the native
 * hadoop.dll library. Permissions are not used on a local Windows machine, so permission
 * changes are skipped and directory listing is done with plain Java.
 */
public class NoPermissionLocalFileSystem extends RawLocalFileSystem {

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        // no-op: permissions are not used locally on Windows
    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws IOException {
        // no-op: ownership is not used locally on Windows
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
        if (!file.isDirectory()) {
            return new FileStatus[] {getFileStatus(path)};
        }

        String[] names = file.list();
        if (names == null) {
            throw new IOException("Cannot list directory " + path);
        }

        List<FileStatus> statuses = new ArrayList<>();
        for (String name : names) {
            try {
                statuses.add(getFileStatus(new Path(path, name)));
            } catch (FileNotFoundException e) {
                // the file was removed while listing the directory
            }
        }
        return statuses.toArray(new FileStatus[0]);
    }
}
