using Fsp;
using Fsp.Interop;
using FspFileInfo = Fsp.Interop.FileInfo;
using FspVolumeInfo = Fsp.Interop.VolumeInfo;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Text;
using System.Threading;

internal sealed class CxnfsWinFsp
{
    private const uint FileAttributeReadonly = 0x00000001;
    private const uint FileAttributeDirectory = 0x00000010;
    private const uint FileAttributeArchive = 0x00000020;
    private const uint FileAttributeNormal = 0x00000080;
    private const string StagingDirEnv = "CXNFS_FUSE_STAGING_DIR";
    private const string WinFspStagingDirEnv = "CXNFS_WINFSP_STAGING_DIR";
    private const string FallbackStagingDirEnv = "CXNFS_FUSE_FALLBACK_STAGING_DIR";
    private const string WinFspFallbackStagingDirEnv = "CXNFS_WINFSP_FALLBACK_STAGING_DIR";
    private const int BridgeMaxChunkBytes = 16 * 1024 * 1024;

    private static int Main(string[] args)
    {
        string handoffPath = ParseHandoffPath(args);
        if (handoffPath == null)
        {
            PrintUsage();
            return IsHelp(args) ? 0 : 2;
        }

        MountHandoff handoff;
        try
        {
            handoff = MountHandoff.Read(handoffPath);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("cxnfs-winfsp: failed to read handoff: " + e.Message);
            return 2;
        }

        string username = PromptUsername();
        string password = PromptPassword();
        if (String.IsNullOrWhiteSpace(username) || String.IsNullOrEmpty(password))
        {
            Console.Error.WriteLine("cxnfs-winfsp: invalid credentials");
            return 2;
        }

        BridgeClient bridge = null;
        FileSystemHost host = null;
        CxnfsFileSystem fileSystem = null;
        try
        {
            bridge = BridgeClient.Start(handoff.HandoffPath, username, password);
            fileSystem = new CxnfsFileSystem(handoff, bridge);
            host = new FileSystemHost(fileSystem);
            ConfigureHost(host);
            int status = host.Mount(handoff.MountPoint, null, true, 0);
            if (status != FileSystemBase.STATUS_SUCCESS)
            {
                Console.Error.WriteLine("cxnfs-winfsp: mount failed status=0x" + status.ToString("x8"));
                return 1;
            }

            Console.WriteLine("cxnfs-winfsp: mounted " + handoff.RemoteRoot + " at " + host.MountPoint());
            fileSystem.WaitForStop();
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("cxnfs-winfsp: " + e.Message);
            return 1;
        }
        finally
        {
            if (host != null)
                host.Dispose();
            if (bridge != null)
                bridge.Dispose();
            if (password != null)
                ZeroStringBestEffort(password);
        }
    }

    private static string ParseHandoffPath(string[] args)
    {
        if (args.Length == 1 && !IsHelp(args))
            return args[0];
        if (args.Length == 2 && args[0] == "--handoff" && !String.IsNullOrWhiteSpace(args[1]))
            return args[1];
        return null;
    }

    private static bool IsHelp(string[] args)
    {
        return args.Length == 1 && (args[0] == "--help" || args[0] == "help" || args[0] == "-h");
    }

    private static void PrintUsage()
    {
        Console.WriteLine("cxnfs-winfsp --handoff <handoff-report>");
    }

    private static void ConfigureHost(FileSystemHost host)
    {
        host.SectorSize = 4096;
        host.SectorsPerAllocationUnit = 1;
        host.MaxComponentLength = 255;
        host.FileInfoTimeout = 0;
        host.VolumeInfoTimeout = 0;
        host.DirInfoTimeout = 0;
        host.SecurityTimeout = 0;
        host.CaseSensitiveSearch = false;
        host.CasePreservedNames = true;
        host.UnicodeOnDisk = true;
        host.PersistentAcls = false;
        host.NamedStreams = false;
        host.ExtendedAttributes = false;
        host.FileSystemName = "CXNFS";
    }

    private static string PromptUsername()
    {
        Console.Write("CXNFS username: ");
        return Console.ReadLine();
    }

    private static string PromptPassword()
    {
        Console.Write("CXNFS password: ");
        StringBuilder builder = new StringBuilder();
        for (;;)
        {
            ConsoleKeyInfo key = Console.ReadKey(true);
            if (key.Key == ConsoleKey.Enter)
            {
                Console.WriteLine();
                break;
            }
            if (key.Key == ConsoleKey.Backspace)
            {
                if (builder.Length > 0)
                    builder.Length--;
                continue;
            }
            if (!Char.IsControl(key.KeyChar))
                builder.Append(key.KeyChar);
        }
        return builder.ToString();
    }

    private static void ZeroStringBestEffort(string value)
    {
        if (value == null)
            return;
        try
        {
            unsafe
            {
                fixed (char* chars = value)
                {
                    for (int i = 0; i < value.Length; i++)
                        chars[i] = '\0';
                }
            }
        }
        catch
        {
        }
    }

    private sealed class MountHandoff
    {
        public string HandoffPath;
        public string RemoteRoot = "/";
        public string MountPoint;

        public static MountHandoff Read(string path)
        {
            MountHandoff handoff = new MountHandoff();
            handoff.HandoffPath = Path.GetFullPath(path);
            foreach (string rawLine in File.ReadAllLines(path))
            {
                string line = rawLine.TrimEnd('\r', '\n');
                if (line.StartsWith("remote_root=", StringComparison.Ordinal))
                    handoff.RemoteRoot = NormalizeNamespace(line.Substring("remote_root=".Length));
                else if (line.StartsWith("mount_point=", StringComparison.Ordinal))
                    handoff.MountPoint = line.Substring("mount_point=".Length);
            }
            if (String.IsNullOrWhiteSpace(handoff.MountPoint))
                throw new IOException("handoff is missing mount_point");
            if (String.IsNullOrWhiteSpace(handoff.RemoteRoot))
                handoff.RemoteRoot = "/";
            return handoff;
        }
    }

    private sealed class CxnfsFileSystem : FileSystemBase
    {
        private readonly MountHandoff handoff;
        private readonly BridgeClient bridge;
        private readonly string stagingRoot;
        private readonly string fallbackStagingRoot;
        private readonly string stagingMountName;
        private readonly byte[] securityDescriptor;
        private readonly ManualResetEventSlim stopped = new ManualResetEventSlim(false);
        private readonly object localLock = new object();
        private readonly Dictionary<string, LocalEntry> localEntries =
            new Dictionary<string, LocalEntry>(StringComparer.OrdinalIgnoreCase);

        public CxnfsFileSystem(MountHandoff handoff, BridgeClient bridge)
        {
            this.handoff = handoff;
            this.bridge = bridge;
            this.stagingRoot = StagingRootFor(handoff);
            this.fallbackStagingRoot = FallbackStagingRoot();
            this.stagingMountName = StagingMountName(handoff.MountPoint);
            this.securityDescriptor = CreateSecurityDescriptor();
        }

        public void WaitForStop()
        {
            Console.CancelKeyPress += delegate(object sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;
                stopped.Set();
            };
            stopped.Wait();
        }

        public override void DispatcherStopped(bool normally)
        {
            stopped.Set();
        }

        public override int ExceptionHandler(Exception ex)
        {
            BridgeException bridgeException = ex as BridgeException;
            if (bridgeException != null)
                return bridgeException.Status;
            IOException io = ex as IOException;
            if (io != null)
                return NtStatusFromWin32(5);
            return FileSystemBase.STATUS_UNSUCCESSFUL;
        }

        public override int GetVolumeInfo(out FspVolumeInfo volumeInfo)
        {
            volumeInfo = new FspVolumeInfo();
            volumeInfo.TotalSize = 1024UL * 1024UL * 1024UL * 1024UL;
            volumeInfo.FreeSize = 512UL * 1024UL * 1024UL * 1024UL;
            volumeInfo.SetVolumeLabel("CXNFS");
            return STATUS_SUCCESS;
        }

        public override int GetSecurityByName(string fileName, out uint fileAttributes, ref byte[] descriptor)
        {
            CxnfsMetadata metadata;
            int status = TryStat(fileName, out metadata);
            if (status != STATUS_SUCCESS)
            {
                fileAttributes = 0;
                return status;
            }
            fileAttributes = Attributes(metadata.Directory);
            if (descriptor != null)
                descriptor = (byte[])securityDescriptor.Clone();
            return STATUS_SUCCESS;
        }

        public override int Create(
            string fileName,
            uint createOptions,
            uint grantedAccess,
            uint fileAttributes,
            byte[] descriptor,
            ulong allocationSize,
            out object fileNode,
            out object fileDesc,
            out FspFileInfo fileInfo,
            out string normalizedName)
        {
            bool directory = (createOptions & FILE_DIRECTORY_FILE) != 0;
            FileHandle handle = new FileHandle(
                NormalizeWinPath(fileName),
                RemotePath(fileName),
                directory,
                stagingRoot,
                fallbackStagingRoot,
                stagingMountName);
            if (directory)
            {
                lock (localLock)
                    localEntries[handle.LocalPath] = LocalEntry.Directory();
            }
            else
            {
                handle.EnsureTemp(false);
                if (allocationSize > 0)
                {
                    handle.EnsureCapacity((long)allocationSize);
                    handle.SetLength((long)allocationSize);
                }
                handle.Dirty = true;
                PutLocalFile(handle.LocalPath, handle.Size);
            }

            fileNode = handle;
            fileDesc = handle;
            normalizedName = fileName;
            fileInfo = ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override int Open(
            string fileName,
            uint createOptions,
            uint grantedAccess,
            out object fileNode,
            out object fileDesc,
            out FspFileInfo fileInfo,
            out string normalizedName)
        {
            CxnfsMetadata metadata;
            int status = TryStat(fileName, out metadata);
            if (status != STATUS_SUCCESS)
            {
                fileNode = null;
                fileDesc = null;
                fileInfo = new FspFileInfo();
                normalizedName = null;
                return status;
            }
            if (metadata.Directory && (createOptions & FILE_NON_DIRECTORY_FILE) != 0)
            {
                fileNode = null;
                fileDesc = null;
                fileInfo = new FspFileInfo();
                normalizedName = null;
                return STATUS_FILE_IS_A_DIRECTORY;
            }

            FileHandle handle = new FileHandle(
                NormalizeWinPath(fileName),
                RemotePath(fileName),
                metadata.Directory,
                stagingRoot,
                fallbackStagingRoot,
                stagingMountName);
            handle.RemoteSize = metadata.Size;
            handle.MtimeMs = metadata.MtimeMs;
            fileNode = handle;
            fileDesc = handle;
            normalizedName = fileName;
            fileInfo = ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override void Cleanup(object fileNode, object fileDesc, string fileName, uint flags)
        {
            FileHandle handle = fileDesc as FileHandle;
            if (handle == null)
                return;
            try
            {
                if ((flags & CleanupDelete) != 0)
                {
                    DeletePath(fileName, handle.Directory);
                    return;
                }
                Commit(handle);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("cxnfs-winfsp: cleanup failed: " + e.Message);
            }
        }

        public override void Close(object fileNode, object fileDesc)
        {
            FileHandle handle = fileDesc as FileHandle;
            if (handle != null)
                handle.Dispose();
        }

        public override int Read(
            object fileNode,
            object fileDesc,
            IntPtr buffer,
            ulong offset,
            uint length,
            out uint bytesTransferred)
        {
            FileHandle handle = (FileHandle)fileDesc;
            bytesTransferred = 0;
            if (handle.Directory)
                return STATUS_FILE_IS_A_DIRECTORY;
            if (length == 0)
                return STATUS_SUCCESS;

            byte[] data;
            if (handle.Temp != null)
            {
                if (offset >= (ulong)handle.Temp.Length)
                    return STATUS_SUCCESS;
                int count = (int)Math.Min(length, (ulong)(handle.Temp.Length - (long)offset));
                data = new byte[count];
                handle.Temp.Position = (long)offset;
                int read = handle.Temp.Read(data, 0, count);
                bytesTransferred = (uint)read;
                Marshal.Copy(data, 0, buffer, read);
                return STATUS_SUCCESS;
            }

            data = bridge.ReadRange(handle.RemotePath, (long)offset, (int)length);
            bytesTransferred = (uint)data.Length;
            if (data.Length > 0)
                Marshal.Copy(data, 0, buffer, data.Length);
            return STATUS_SUCCESS;
        }

        public override int Write(
            object fileNode,
            object fileDesc,
            IntPtr buffer,
            ulong offset,
            uint length,
            bool writeToEndOfFile,
            bool constrainedIo,
            out uint bytesTransferred,
            out FspFileInfo fileInfo)
        {
            FileHandle handle = (FileHandle)fileDesc;
            bytesTransferred = 0;
            fileInfo = ToFileInfo(handle);
            if (handle.Directory)
                return STATUS_FILE_IS_A_DIRECTORY;
            if (handle.Temp == null)
            {
                handle.EnsureTemp(false);
                long checkOffset = writeToEndOfFile ? (long)handle.RemoteSize : (long)offset;
                if (checkOffset != 0 && handle.RemoteSize > 0)
                {
                    EnsureHydrated(handle);
                }
            }
            byte[] data = new byte[length];
            if (length > 0)
                Marshal.Copy(buffer, data, 0, (int)length);
            long targetOffset = writeToEndOfFile ? handle.Temp.Length : (long)offset;
            if (constrainedIo && targetOffset + data.Length > handle.Temp.Length)
            {
                data = Slice(data, 0, Math.Max(0, (int)(handle.Temp.Length - targetOffset)));
            }
            handle.EnsureCapacity(targetOffset + data.Length);
            handle.Temp.Position = targetOffset;
            handle.WriteTemp(data, 0, data.Length);
            if (data.Length > 0)
            {
                if (!handle.StreamDisabled && targetOffset == handle.StreamNextOffset)
                {
                    bridge.WriteChunk(handle.RemotePath, targetOffset, data, data.Length);
                    handle.StreamStarted = true;
                    handle.StreamNextOffset += data.Length;
                }
                else
                {
                    handle.StreamDisabled = true;
                }
            }
            handle.Dirty = true;
            bytesTransferred = (uint)data.Length;
            PutLocalFile(handle.LocalPath, handle.Size);
            fileInfo = ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override int Flush(object fileNode, object fileDesc, out FspFileInfo fileInfo)
        {
            FileHandle handle = fileDesc as FileHandle;
            if (handle != null)
                Commit(handle);
            fileInfo = handle == null ? new FspFileInfo() : ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override int GetFileInfo(object fileNode, object fileDesc, out FspFileInfo fileInfo)
        {
            FileHandle handle = fileDesc as FileHandle;
            if (handle != null)
            {
                fileInfo = ToFileInfo(handle);
                return STATUS_SUCCESS;
            }
            fileInfo = new FspFileInfo();
            return STATUS_INVALID_HANDLE;
        }

        public override int SetBasicInfo(
            object fileNode,
            object fileDesc,
            uint fileAttributes,
            ulong creationTime,
            ulong lastAccessTime,
            ulong lastWriteTime,
            ulong changeTime,
            out FspFileInfo fileInfo)
        {
            FileHandle handle = (FileHandle)fileDesc;
            if (lastWriteTime != 0)
                handle.MtimeMs = FileTimeToUnixMs(lastWriteTime);
            fileInfo = ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override int SetFileSize(
            object fileNode,
            object fileDesc,
            ulong newSize,
            bool setAllocationSize,
            out FspFileInfo fileInfo)
        {
            FileHandle handle = (FileHandle)fileDesc;
            if (handle.Directory)
            {
                fileInfo = ToFileInfo(handle);
                return STATUS_FILE_IS_A_DIRECTORY;
            }
            EnsureHydrated(handle);
            handle.EnsureCapacity((long)newSize);
            handle.SetLength((long)newSize);
            if (handle.StreamStarted && (long)newSize != handle.StreamNextOffset)
                handle.StreamDisabled = true;
            handle.Dirty = true;
            PutLocalFile(handle.LocalPath, handle.Size);
            fileInfo = ToFileInfo(handle);
            return STATUS_SUCCESS;
        }

        public override int CanDelete(object fileNode, object fileDesc, string fileName)
        {
            return STATUS_SUCCESS;
        }

        public override int Rename(
            object fileNode,
            object fileDesc,
            string fileName,
            string newFileName,
            bool replaceIfExists)
        {
            bridge.Rename(RemotePath(fileName), RemotePath(newFileName));
            lock (localLock)
            {
                string oldLocal = NormalizeWinPath(fileName);
                string newLocal = NormalizeWinPath(newFileName);
                LocalEntry entry;
                if (localEntries.TryGetValue(oldLocal, out entry))
                {
                    localEntries.Remove(oldLocal);
                    localEntries[newLocal] = entry;
                }
            }
            return STATUS_SUCCESS;
        }

        public override int GetSecurity(object fileNode, object fileDesc, ref byte[] descriptor)
        {
            descriptor = (byte[])securityDescriptor.Clone();
            return STATUS_SUCCESS;
        }

        public override bool ReadDirectoryEntry(
            object fileNode,
            object fileDesc,
            string pattern,
            string marker,
            ref object context,
            out string fileName,
            out FspFileInfo fileInfo)
        {
            DirectoryCursor cursor = context as DirectoryCursor;
            if (cursor == null)
            {
                FileHandle handle = (FileHandle)fileDesc;
                cursor = new DirectoryCursor(ListDirectory(handle.LocalPath, handle.RemotePath, marker));
                context = cursor;
            }
            DirectoryEntry entry = cursor.Next();
            if (entry == null)
            {
                fileName = null;
                fileInfo = new FspFileInfo();
                return false;
            }
            fileName = entry.Name;
            fileInfo = ToFileInfo(entry.Metadata);
            return true;
        }

        private int TryStat(string fileName, out CxnfsMetadata metadata)
        {
            string local = NormalizeWinPath(fileName);
            lock (localLock)
            {
                LocalEntry entry;
                if (localEntries.TryGetValue(local, out entry))
                {
                    metadata = entry.Metadata;
                    return STATUS_SUCCESS;
                }
            }
            try
            {
                metadata = bridge.Stat(RemotePath(fileName));
                return STATUS_SUCCESS;
            }
            catch (BridgeException e)
            {
                metadata = null;
                return e.Status;
            }
        }

        private List<DirectoryEntry> ListDirectory(string localPath, string remotePath, string marker)
        {
            List<DirectoryEntry> entries = new List<DirectoryEntry>();
            lock (localLock)
            {
                foreach (KeyValuePair<string, LocalEntry> item in localEntries)
                {
                    string name;
                    if (ParentName(item.Key, localPath, out name))
                        entries.Add(new DirectoryEntry(name, item.Value.Metadata));
                }
            }
            try
            {
                entries.AddRange(bridge.List(remotePath));
            }
            catch (BridgeException)
            {
                if (entries.Count == 0)
                    throw;
            }
            entries.Sort(delegate(DirectoryEntry a, DirectoryEntry b)
            {
                return StringComparer.OrdinalIgnoreCase.Compare(a.Name, b.Name);
            });
            if (!String.IsNullOrEmpty(marker))
                entries.RemoveAll(e => StringComparer.OrdinalIgnoreCase.Compare(e.Name, marker) <= 0);
            return entries;
        }

        private void DeletePath(string fileName, bool directory)
        {
            string local = NormalizeWinPath(fileName);
            if (!directory)
            {
                lock (localLock)
                    localEntries.Remove(local);
            }
            else
            {
                lock (localLock)
                    localEntries.Remove(local);
            }
            bridge.Delete(RemotePath(fileName), directory);
        }

        private void EnsureHydrated(FileHandle handle)
        {
            if (handle.Temp != null)
                return;
            handle.EnsureTemp(false);
            if (handle.RemoteSize > 0)
            {
                const int chunk = 1024 * 1024;
                long offset = 0;
                while (offset < handle.RemoteSize)
                {
                    int length = (int)Math.Min(chunk, handle.RemoteSize - offset);
                    byte[] data = bridge.ReadRange(handle.RemotePath, offset, length);
                    if (data.Length == 0)
                        break;
                    handle.WriteTemp(data, 0, data.Length);
                    offset += data.Length;
                }
                handle.Temp.Position = 0;
            }
        }

        private void Commit(FileHandle handle)
        {
            if (handle == null || handle.Directory || !handle.Dirty)
                return;
            handle.Temp.Flush();
            if (!handle.StreamDisabled && handle.StreamStarted && handle.Size == handle.StreamNextOffset)
                bridge.CommitWrite(handle.RemotePath, handle.Size);
            else if (!handle.StreamDisabled && !handle.StreamStarted && handle.Size == 0)
                bridge.CommitWrite(handle.RemotePath, 0);
            else
                bridge.WriteFileAsChunks(handle.RemotePath, handle.TempPath);
            handle.RemoteSize = handle.Size;
            handle.MtimeMs = NowMs();
            handle.Dirty = false;
            handle.StreamDisabled = true;
            lock (localLock)
                localEntries.Remove(handle.LocalPath);
        }

        private void PutLocalFile(string localPath, long size)
        {
            lock (localLock)
                localEntries[localPath] = LocalEntry.File(size);
        }

        private string RemotePath(string fileName)
        {
            string local = NormalizeWinPath(fileName);
            if (local == "/")
                return handoff.RemoteRoot;
            if (handoff.RemoteRoot == "/")
                return local;
            return handoff.RemoteRoot + local;
        }

        private static bool ParentName(string child, string parent, out string name)
        {
            name = null;
            if (child == "/" || child == parent)
                return false;
            int slash = child.LastIndexOf('/');
            string childParent = slash <= 0 ? "/" : child.Substring(0, slash);
            if (!String.Equals(childParent, parent, StringComparison.OrdinalIgnoreCase))
                return false;
            name = child.Substring(slash + 1);
            return name.Length > 0;
        }

        private static string StagingRootFor(MountHandoff handoff)
        {
            string configured = NonEmptyEnvironment(WinFspStagingDirEnv);
            if (configured == null)
                configured = NonEmptyEnvironment(StagingDirEnv);
            if (configured != null)
                return Path.GetFullPath(configured);

            if (IsDriveLetterMount(handoff.MountPoint))
                return Path.GetTempPath();
            try
            {
                string full = Path.GetFullPath(handoff.MountPoint);
                string trimmed = full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                DirectoryInfo parent = Directory.GetParent(trimmed);
                if (parent != null)
                    return parent.FullName;
            }
            catch
            {
            }
            return Path.GetTempPath();
        }

        private static string FallbackStagingRoot()
        {
            string configured = NonEmptyEnvironment(WinFspFallbackStagingDirEnv);
            if (configured == null)
                configured = NonEmptyEnvironment(FallbackStagingDirEnv);
            return configured == null ? Path.GetTempPath() : Path.GetFullPath(configured);
        }

        private static string StagingMountName(string mountPoint)
        {
            if (IsDriveLetterMount(mountPoint))
                return Char.ToUpperInvariant(mountPoint[0]).ToString();
            try
            {
                string full = Path.GetFullPath(mountPoint);
                string trimmed = full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                string name = Path.GetFileName(trimmed);
                if (!String.IsNullOrWhiteSpace(name) && name != "." && name != "..")
                    return name;
            }
            catch
            {
            }
            return "mount";
        }

        private static bool IsDriveLetterMount(string mountPoint)
        {
            return !String.IsNullOrWhiteSpace(mountPoint)
                && mountPoint.Length == 2
                && mountPoint[1] == ':'
                && Char.IsLetter(mountPoint[0]);
        }

        private static string NonEmptyEnvironment(string name)
        {
            string value = Environment.GetEnvironmentVariable(name);
            return String.IsNullOrWhiteSpace(value) ? null : value;
        }

        private static byte[] CreateSecurityDescriptor()
        {
            RawSecurityDescriptor descriptor = new RawSecurityDescriptor(
                "O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)");
            byte[] bytes = new byte[descriptor.BinaryLength];
            descriptor.GetBinaryForm(bytes, 0);
            return bytes;
        }

        private static FspFileInfo ToFileInfo(FileHandle handle)
        {
            CxnfsMetadata metadata = new CxnfsMetadata(handle.Directory, handle.Size, handle.MtimeMs);
            return ToFileInfo(metadata);
        }

        private static FspFileInfo ToFileInfo(CxnfsMetadata metadata)
        {
            FspFileInfo info = new FspFileInfo();
            info.FileAttributes = Attributes(metadata.Directory);
            info.AllocationSize = Align((ulong)(metadata.Directory ? 0 : metadata.Size), 4096);
            info.FileSize = (ulong)(metadata.Directory ? 0 : metadata.Size);
            ulong fileTime = UnixMsToFileTime(metadata.MtimeMs);
            info.CreationTime = fileTime;
            info.LastAccessTime = fileTime;
            info.LastWriteTime = fileTime;
            info.ChangeTime = fileTime;
            info.IndexNumber = StableIndex(metadata);
            info.HardLinks = 0;
            return info;
        }

        private static uint Attributes(bool directory)
        {
            return directory ? FileAttributeDirectory : FileAttributeArchive;
        }

        private static ulong Align(ulong value, ulong alignment)
        {
            if (value == 0)
                return 0;
            ulong remainder = value % alignment;
            return remainder == 0 ? value : value + alignment - remainder;
        }

        private static ulong UnixMsToFileTime(long unixMs)
        {
            if (unixMs <= 0)
                unixMs = NowMs();
            return (ulong)(unixMs * 10000L + 116444736000000000L);
        }

        private static long FileTimeToUnixMs(ulong fileTime)
        {
            if (fileTime <= 116444736000000000UL)
                return NowMs();
            return (long)((fileTime - 116444736000000000UL) / 10000UL);
        }

        private static ulong StableIndex(CxnfsMetadata metadata)
        {
            unchecked
            {
                return (ulong)((metadata.Directory ? 1469598103934665603L : 1099511628211L)
                    ^ metadata.Size ^ metadata.MtimeMs);
            }
        }
    }

    private sealed class BridgeClient : IDisposable
    {
        private readonly Process process;
        private readonly Stream input;
        private readonly Stream output;
        private readonly object gate = new object();

        private BridgeClient(Process process)
        {
            this.process = process;
            this.input = process.StandardInput.BaseStream;
            this.output = process.StandardOutput.BaseStream;
        }

        public static BridgeClient Start(string handoffPath, string username, string password)
        {
            ProcessStartInfo info = BridgeProcessStartInfo(handoffPath);
            info.UseShellExecute = false;
            info.RedirectStandardInput = true;
            info.RedirectStandardOutput = true;
            info.RedirectStandardError = true;
            Process process = Process.Start(info);
            process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                if (e.Data != null)
                    Console.Error.WriteLine(e.Data);
            };
            process.BeginErrorReadLine();
            BridgeClient bridge = new BridgeClient(process);
            bridge.WriteAscii("AUTH\t" + username + "\t" + password + "\n");
            for (;;)
            {
                string line = bridge.ReadLine();
                if (line == null)
                    throw new IOException("bridge did not become ready");
                if (line == "READY")
                    return bridge;
                Console.Error.WriteLine("cxnfs-winfsp: ignoring bridge startup output: " + line);
            }
        }

        private static ProcessStartInfo BridgeProcessStartInfo(string handoffPath)
        {
            string launcher = ResolveCxnfsLauncher();
            string arguments = "fuse-bridge " + Quote(handoffPath) + " --credentials-stdin";
            ProcessStartInfo info = new ProcessStartInfo();
            if (IsCommandScript(launcher))
            {
                info.FileName = ResolveCommandProcessor();
                info.Arguments = "/d /s /c \"" + CmdQuote(launcher) + " " + arguments + "\"";
            }
            else
            {
                info.FileName = launcher;
                info.Arguments = arguments;
            }
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            if (!String.IsNullOrWhiteSpace(baseDirectory) && Directory.Exists(baseDirectory))
                info.WorkingDirectory = baseDirectory;
            return info;
        }

        private static string ResolveCxnfsLauncher()
        {
            string configured = Environment.GetEnvironmentVariable("CXNFS_CMD");
            if (!String.IsNullOrWhiteSpace(configured))
                return configured;
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            if (!String.IsNullOrWhiteSpace(baseDirectory))
            {
                string localCmd = Path.Combine(baseDirectory, "cxnfs.cmd");
                if (File.Exists(localCmd))
                    return localCmd;
                string localExe = Path.Combine(baseDirectory, "cxnfs.exe");
                if (File.Exists(localExe))
                    return localExe;
            }
            return "cxnfs.cmd";
        }

        private static string ResolveCommandProcessor()
        {
            string comspec = Environment.GetEnvironmentVariable("COMSPEC");
            return String.IsNullOrWhiteSpace(comspec) ? "cmd.exe" : comspec;
        }

        private static bool IsCommandScript(string launcher)
        {
            return launcher.EndsWith(".cmd", StringComparison.OrdinalIgnoreCase)
                || launcher.EndsWith(".bat", StringComparison.OrdinalIgnoreCase)
                || launcher.Equals("cxnfs.cmd", StringComparison.OrdinalIgnoreCase)
                || launcher.Equals("cxnfs.bat", StringComparison.OrdinalIgnoreCase);
        }

        public CxnfsMetadata Stat(string remotePath)
        {
            lock (gate)
            {
                WriteAscii("STAT\t" + remotePath + "\n");
                string line = RequireLine();
                if (line == "NOT_FOUND")
                    throw new BridgeException(FileSystemBase.STATUS_OBJECT_NAME_NOT_FOUND, "not found: " + remotePath);
                if (line.StartsWith("ERR\t", StringComparison.Ordinal))
                    throw Error(line.Substring(4));
                string[] parts = line.Split('\t');
                if (parts.Length < 4 || parts[0] != "OK")
                    throw new IOException("invalid stat response: " + line);
                return new CxnfsMetadata(parts[1] == "DIRECTORY", ParseLong(parts[2]), ParseLong(parts[3]));
            }
        }

        public List<DirectoryEntry> List(string remotePath)
        {
            lock (gate)
            {
                WriteAscii("LIST\t" + remotePath + "\n");
                string line = RequireLine();
                if (line.StartsWith("ERR\t", StringComparison.Ordinal))
                    throw Error(line.Substring(4));
                if (!line.StartsWith("OK", StringComparison.Ordinal))
                    throw new IOException("invalid list response: " + line);
                List<DirectoryEntry> entries = new List<DirectoryEntry>();
                while ((line = RequireLine()) != "END")
                {
                    if (!line.StartsWith("ENTRY\t", StringComparison.Ordinal))
                        throw new IOException("invalid directory entry response: " + line);
                    string[] parts = line.Split('\t');
                    if (parts.Length < 5)
                        throw new IOException("invalid directory entry response: " + line);
                    entries.Add(new DirectoryEntry(
                        parts[4],
                        new CxnfsMetadata(parts[1] == "DIRECTORY", ParseLong(parts[2]), ParseLong(parts[3]))));
                }
                return entries;
            }
        }

        public byte[] ReadRange(string remotePath, long offset, int length)
        {
            lock (gate)
            {
                WriteAscii("READ\t" + remotePath + "\t" + offset + "\t" + length + "\n");
                string line = RequireLine();
                if (line.StartsWith("ERR\t", StringComparison.Ordinal))
                    throw Error(line.Substring(4));
                if (!line.StartsWith("DATA\t", StringComparison.Ordinal))
                    throw new IOException("invalid read response: " + line);
                int dataLength = (int)ParseLong(line.Substring(5));
                if (dataLength < 0 || dataLength > length)
                    throw new IOException("invalid read length: " + dataLength);
                byte[] data = ReadExact(dataLength);
                int newline = output.ReadByte();
                if (newline != '\n')
                    throw new IOException("invalid read frame terminator");
                line = RequireLine();
                if (line != "END")
                    throw new IOException("invalid read end marker: " + line);
                return data;
            }
        }

        public void WriteChunk(string remotePath, long offset, byte[] data, int length)
        {
            if (length <= 0)
                return;
            if (length > BridgeMaxChunkBytes)
            {
                int cursor = 0;
                while (cursor < length)
                {
                    int count = Math.Min(BridgeMaxChunkBytes, length - cursor);
                    byte[] slice = new byte[count];
                    Array.Copy(data, cursor, slice, 0, count);
                    WriteChunk(remotePath, offset + cursor, slice, count);
                    cursor += count;
                }
                return;
            }
            lock (gate)
            {
                WriteAscii("WRITE_CHUNK\t" + remotePath + "\t" + offset + "\t" + length + "\n");
                input.Write(data, 0, length);
                input.Flush();
                ReadOk("write chunk");
            }
        }

        public void CommitWrite(string remotePath, long length)
        {
            lock (gate)
            {
                WriteAscii("WRITE_COMMIT\t" + remotePath + "\t" + length + "\n");
                ReadOk("write commit");
            }
        }

        public void AbortWrite(string remotePath)
        {
            lock (gate)
            {
                WriteAscii("WRITE_ABORT\t" + remotePath + "\n");
                ReadOk("write abort");
            }
        }

        public void WriteFileAsChunks(string remotePath, string localPath)
        {
            System.IO.FileInfo info = new System.IO.FileInfo(localPath);
            long offset = 0;
            using (FileStream file = File.OpenRead(localPath))
            {
                byte[] buffer = new byte[BridgeMaxChunkBytes];
                int read;
                while ((read = file.Read(buffer, 0, buffer.Length)) > 0)
                {
                    byte[] payload = buffer;
                    if (read != buffer.Length)
                    {
                        payload = new byte[read];
                        Array.Copy(buffer, 0, payload, 0, read);
                    }
                    WriteChunk(remotePath, offset, payload, read);
                    offset += read;
                }
            }
            if (offset != info.Length)
                throw new IOException("staged file changed while streaming commit");
            CommitWrite(remotePath, info.Length);
        }

        public void Delete(string remotePath, bool recursive)
        {
            lock (gate)
            {
                WriteAscii("DELETE\t" + remotePath + "\t" + (recursive ? "true" : "false") + "\n");
                ReadOk("delete");
            }
        }

        public void Rename(string source, string target)
        {
            lock (gate)
            {
                WriteAscii("RENAME\t" + source + "\t" + target + "\n");
                ReadOk("rename");
            }
        }

        public void Dispose()
        {
            try
            {
                WriteAscii("QUIT\n");
            }
            catch
            {
            }
            try
            {
                process.WaitForExit(2000);
            }
            catch
            {
            }
            if (!process.HasExited)
            {
                try { process.Kill(); } catch { }
            }
            process.Dispose();
        }

        private void ReadOk(string operation)
        {
            string line = RequireLine();
            if (line == "OK" || line.StartsWith("OK\t", StringComparison.Ordinal))
                return;
            if (line.StartsWith("ERR\t", StringComparison.Ordinal))
                throw Error(line.Substring(4));
            throw new IOException("invalid " + operation + " response: " + line);
        }

        private BridgeException Error(string message)
        {
            string lower = message.ToLowerInvariant();
            if (lower.Contains("not found") || lower.Contains("unknown") || lower.Contains("missing"))
                return new BridgeException(FileSystemBase.STATUS_OBJECT_NAME_NOT_FOUND, message);
            if (lower.Contains("permission") || lower.Contains("not authorized"))
                return new BridgeException(FileSystemBase.STATUS_ACCESS_DENIED, message);
            if (lower.Contains("already exists"))
                return new BridgeException(FileSystemBase.STATUS_OBJECT_NAME_COLLISION, message);
            if (lower.Contains("not empty"))
                return new BridgeException(FileSystemBase.STATUS_DIRECTORY_NOT_EMPTY, message);
            return new BridgeException(FileSystemBase.STATUS_UNSUCCESSFUL, message);
        }

        private string RequireLine()
        {
            string line = ReadLine();
            if (line == null)
                throw new IOException("bridge stream ended");
            return line;
        }

        private string ReadLine()
        {
            List<byte> bytes = new List<byte>();
            for (;;)
            {
                int value = output.ReadByte();
                if (value < 0)
                    return bytes.Count == 0 ? null : Encoding.UTF8.GetString(bytes.ToArray());
                if (value == '\n')
                    return Encoding.UTF8.GetString(bytes.ToArray()).TrimEnd('\r');
                bytes.Add((byte)value);
            }
        }

        private byte[] ReadExact(int length)
        {
            byte[] data = new byte[length];
            int offset = 0;
            while (offset < length)
            {
                int read = output.Read(data, offset, length - offset);
                if (read <= 0)
                    throw new IOException("bridge stream ended before payload");
                offset += read;
            }
            return data;
        }

        private void WriteAscii(string text)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            input.Write(bytes, 0, bytes.Length);
            input.Flush();
        }

        private static string Quote(string value)
        {
            return "\"" + value.Replace("\"", "\\\"") + "\"";
        }

        private static string CmdQuote(string value)
        {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
    }

    private sealed class FileHandle : IDisposable
    {
        public readonly string LocalPath;
        public readonly string RemotePath;
        public readonly bool Directory;
        private readonly string stagingRoot;
        private readonly string fallbackStagingRoot;
        private readonly string stagingMountName;
        public long RemoteSize;
        public long MtimeMs;
        public bool Dirty;
        public string TempPath;
        public string TempDirectory;
        public string TempCleanupStop;
        public bool UsingFallbackStaging;
        public FileStream Temp;
        public bool StreamStarted;
        public bool StreamDisabled;
        public long StreamNextOffset;

        public FileHandle(
            string localPath,
            string remotePath,
            bool directory,
            string stagingRoot,
            string fallbackStagingRoot,
            string stagingMountName)
        {
            LocalPath = localPath;
            RemotePath = remotePath;
            Directory = directory;
            this.stagingRoot = stagingRoot;
            this.fallbackStagingRoot = fallbackStagingRoot;
            this.stagingMountName = stagingMountName;
            MtimeMs = NowMs();
        }

        public long Size
        {
            get
            {
                if (Directory)
                    return 0;
                return Temp != null ? Temp.Length : RemoteSize;
            }
        }

        public void EnsureTemp(bool truncate)
        {
            if (Temp != null)
            {
                if (truncate)
                    SetLength(0);
                return;
            }
            try
            {
                CreateTemp(false);
            }
            catch (Exception)
            {
                if (StagingRootsMatch())
                    throw;
                CreateTemp(true);
            }
            if (truncate)
                SetLength(0);
        }

        public void EnsureCapacity(long targetSize)
        {
            if (Temp == null || targetSize <= Temp.Length || UsingFallbackStaging || StagingRootsMatch())
                return;
            long additional = targetSize - Temp.Length;
            long available = AvailableBytesFor(TempPath);
            if (available >= 0 && available < additional)
                MigrateToFallback();
        }

        public void SetLength(long length)
        {
            try
            {
                Temp.SetLength(length);
            }
            catch (IOException)
            {
                if (!TryMigrateToFallback())
                    throw;
                Temp.SetLength(length);
            }
        }

        public void WriteTemp(byte[] data, int offset, int count)
        {
            long position = Temp.Position;
            try
            {
                Temp.Write(data, offset, count);
            }
            catch (IOException)
            {
                if (!TryMigrateToFallback())
                    throw;
                Temp.Position = position;
                Temp.Write(data, offset, count);
            }
        }

        private void CreateTemp(bool useFallback)
        {
            string root = useFallback ? fallbackStagingRoot : stagingRoot;
            string baseDirectory = Path.Combine(root, ".tmp");
            string relative = SafeRelativePath(LocalPath);
            TempDirectory = Path.Combine(baseDirectory, stagingMountName, relative);
            TempCleanupStop = baseDirectory;
            System.IO.Directory.CreateDirectory(TempDirectory);
            TempPath = Path.Combine(TempDirectory, "cxnfs-winfsp-" + Guid.NewGuid().ToString("N") + ".tmp");
            Temp = new FileStream(TempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            UsingFallbackStaging = useFallback;
        }

        private bool TryMigrateToFallback()
        {
            if (UsingFallbackStaging || StagingRootsMatch())
                return false;
            MigrateToFallback();
            return true;
        }

        private void MigrateToFallback()
        {
            FileStream oldTemp = Temp;
            string oldPath = TempPath;
            string oldDirectory = TempDirectory;
            string oldCleanupStop = TempCleanupStop;
            long position = oldTemp.Position;

            Temp = null;
            TempPath = null;
            TempDirectory = null;
            TempCleanupStop = null;
            try
            {
                CreateTemp(true);
                oldTemp.Position = 0;
                oldTemp.CopyTo(Temp);
                Temp.Position = Math.Min(position, Temp.Length);
                oldTemp.Dispose();
                CleanupTempArtifact(oldPath, oldDirectory, oldCleanupStop);
            }
            catch
            {
                if (Temp != null)
                    Temp.Dispose();
                CleanupTempArtifact(TempPath, TempDirectory, TempCleanupStop);
                Temp = oldTemp;
                TempPath = oldPath;
                TempDirectory = oldDirectory;
                TempCleanupStop = oldCleanupStop;
                throw;
            }
        }

        private bool StagingRootsMatch()
        {
            return String.Equals(
                Path.GetFullPath(stagingRoot).TrimEnd('\\', '/'),
                Path.GetFullPath(fallbackStagingRoot).TrimEnd('\\', '/'),
                StringComparison.OrdinalIgnoreCase);
        }

        private static long AvailableBytesFor(string path)
        {
            try
            {
                string root = Path.GetPathRoot(Path.GetFullPath(path));
                if (String.IsNullOrEmpty(root))
                    return -1;
                return new DriveInfo(root).AvailableFreeSpace;
            }
            catch
            {
                return -1;
            }
        }

        private static string SafeRelativePath(string localPath)
        {
            string relative = localPath == null ? "" : localPath.TrimStart('/', '\\');
            if (String.IsNullOrEmpty(relative))
                return "__root__";
            string[] parts = relative.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);
            for (int index = 0; index < parts.Length; index++)
            {
                if (parts[index] == "." || parts[index] == "..")
                    throw new IOException("invalid staging path: " + localPath);
            }
            return Path.Combine(parts);
        }

        private static void CleanupTempArtifact(string tempPath, string tempDirectory, string cleanupStop)
        {
            if (tempPath != null)
            {
                try { File.Delete(tempPath); } catch { }
            }
            PruneEmptyDirectories(tempDirectory, cleanupStop);
        }

        private static void PruneEmptyDirectories(string start, string stop)
        {
            if (String.IsNullOrEmpty(start) || String.IsNullOrEmpty(stop))
                return;
            string current = start;
            string normalizedStop = Path.GetFullPath(stop).TrimEnd('\\', '/');
            while (!String.IsNullOrEmpty(current))
            {
                string normalizedCurrent = Path.GetFullPath(current).TrimEnd('\\', '/');
                if (String.Equals(normalizedCurrent, normalizedStop, StringComparison.OrdinalIgnoreCase))
                    break;
                try
                {
                    System.IO.Directory.Delete(current, false);
                }
                catch
                {
                    break;
                }
                current = Path.GetDirectoryName(current);
            }
        }

        public void Dispose()
        {
            if (Temp != null)
            {
                Temp.Dispose();
                Temp = null;
            }
            if (TempPath != null)
            {
                CleanupTempArtifact(TempPath, TempDirectory, TempCleanupStop);
                TempPath = null;
            }
            TempDirectory = null;
            TempCleanupStop = null;
        }
    }

    private sealed class LocalEntry
    {
        public readonly CxnfsMetadata Metadata;

        private LocalEntry(CxnfsMetadata metadata)
        {
            Metadata = metadata;
        }

        public static LocalEntry Directory()
        {
            return new LocalEntry(new CxnfsMetadata(true, 0, NowMs()));
        }

        public static LocalEntry File(long size)
        {
            return new LocalEntry(new CxnfsMetadata(false, size, NowMs()));
        }
    }

    private sealed class CxnfsMetadata
    {
        public readonly bool Directory;
        public readonly long Size;
        public readonly long MtimeMs;

        public CxnfsMetadata(bool directory, long size, long mtimeMs)
        {
            Directory = directory;
            Size = size;
            MtimeMs = mtimeMs <= 0 ? NowMs() : mtimeMs;
        }
    }

    private sealed class DirectoryEntry
    {
        public readonly string Name;
        public readonly CxnfsMetadata Metadata;

        public DirectoryEntry(string name, CxnfsMetadata metadata)
        {
            Name = name;
            Metadata = metadata;
        }
    }

    private sealed class DirectoryCursor
    {
        private readonly List<DirectoryEntry> entries;
        private int index;

        public DirectoryCursor(List<DirectoryEntry> entries)
        {
            this.entries = entries;
        }

        public DirectoryEntry Next()
        {
            if (index >= entries.Count)
                return null;
            return entries[index++];
        }
    }

    private sealed class BridgeException : IOException
    {
        public readonly int Status;

        public BridgeException(int status, string message)
            : base(message)
        {
            Status = status;
        }
    }

    private static string NormalizeNamespace(string value)
    {
        if (String.IsNullOrWhiteSpace(value))
            return "/";
        value = value.Replace('\\', '/').Trim();
        if (!value.StartsWith("/", StringComparison.Ordinal))
            value = "/" + value;
        while (value.Length > 1 && value.EndsWith("/", StringComparison.Ordinal))
            value = value.Substring(0, value.Length - 1);
        return value;
    }

    private static string NormalizeWinPath(string fileName)
    {
        if (String.IsNullOrWhiteSpace(fileName) || fileName == "\\")
            return "/";
        string value = fileName.Replace('\\', '/');
        if (!value.StartsWith("/", StringComparison.Ordinal))
            value = "/" + value;
        while (value.Length > 1 && value.EndsWith("/", StringComparison.Ordinal))
            value = value.Substring(0, value.Length - 1);
        return value;
    }

    private static long ParseLong(string value)
    {
        long result;
        return Int64.TryParse(value, out result) ? result : 0;
    }

    private static byte[] Slice(byte[] data, int offset, int length)
    {
        if (length <= 0)
            return new byte[0];
        byte[] result = new byte[length];
        Buffer.BlockCopy(data, offset, result, 0, length);
        return result;
    }

    private static long NowMs()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
