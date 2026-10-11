using System;
using System.Diagnostics;
using System.IO;
using System.ServiceProcess;
using System.Threading;

internal sealed class CxnfsService : ServiceBase
{
    private const string CxnfsServiceName = "cxnfs";
    private const string CxnfsDisplayName = "CXNFS File Server";
    private const string CxnfsDescription = "CXNFS authenticated file server daemon.";

    private Process child;
    private readonly object gate = new object();
    private volatile bool stopping;

    public CxnfsService()
    {
        ServiceName = CxnfsServiceName;
        CanStop = true;
        CanShutdown = true;
        AutoLog = true;
    }

    public static int Main(string[] args)
    {
        if (args.Length > 0)
            return RunCommand(args);
        if (Environment.UserInteractive)
            return RunCommand(new string[] { "help" });
        ServiceBase.Run(new CxnfsService());
        return 0;
    }

    private static int RunCommand(string[] args)
    {
        string command = args[0].ToLowerInvariant();
        try
        {
            if (command == "install")
                return Install();
            if (command == "uninstall" || command == "delete" || command == "remove")
                return Uninstall();
            if (command == "start")
                return RunSc("start " + CxnfsServiceName, false);
            if (command == "stop")
                return StopInstalledService(false);
            if (command == "run")
            {
                CxnfsService service = new CxnfsService();
                service.StartChild();
                Console.WriteLine("cxnfs-service: running; press Ctrl+C to stop");
                ManualResetEventSlim stopped = new ManualResetEventSlim(false);
                Console.CancelKeyPress += delegate(object sender, ConsoleCancelEventArgs eventArgs)
                {
                    eventArgs.Cancel = true;
                    service.StopChild();
                    stopped.Set();
                };
                stopped.Wait();
                return 0;
            }
            PrintUsage();
            return command == "help" || command == "--help" || command == "-h" ? 0 : 2;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("cxnfs-service: " + e.Message);
            return 1;
        }
    }

    private static int Install()
    {
        string executable = Quote(Process.GetCurrentProcess().MainModule.FileName);
        if (ServiceExists())
        {
            int configExit = RunSc("config " + CxnfsServiceName
                    + " binPath= " + executable
                    + " start= auto"
                    + " DisplayName= " + Quote(CxnfsDisplayName),
                true);
            if (configExit != 0)
                return configExit;
        }
        else
        {
            int createExit = RunSc("create " + CxnfsServiceName
                    + " binPath= " + executable
                    + " start= auto"
                    + " DisplayName= " + Quote(CxnfsDisplayName),
                true);
            if (createExit != 0)
                return createExit;
        }

        RunSc("description " + CxnfsServiceName + " " + Quote(CxnfsDescription), false);
        RunSc("failure " + CxnfsServiceName
                + " reset= 3600 actions= restart/2000/restart/2000/restart/2000",
            false);
        Console.WriteLine("Installed CXNFS File Server service. Start with: Start-Service cxnfs");
        return 0;
    }

    private static int Uninstall()
    {
        StopInstalledService(false);
        if (!ServiceExists())
            return 0;
        return RunSc("delete " + CxnfsServiceName, true);
    }

    private static bool ServiceExists()
    {
        return RunSc("query " + CxnfsServiceName, false) == 0;
    }

    private static int StopInstalledService(bool verbose)
    {
        if (!ServiceExists())
            return 0;
        return RunSc("stop " + CxnfsServiceName, verbose);
    }

    private static int RunSc(string arguments, bool verbose)
    {
        using (Process process = new Process())
        {
            process.StartInfo.FileName = "sc.exe";
            process.StartInfo.Arguments = arguments;
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.CreateNoWindow = true;
            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            string error = process.StandardError.ReadToEnd();
            process.WaitForExit();
            if (verbose && output.Length > 0)
                Console.Write(output);
            if (verbose && error.Length > 0)
                Console.Error.Write(error);
            return process.ExitCode;
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("cxnfs-service install|uninstall|start|stop|run");
    }

    protected override void OnStart(string[] args)
    {
        StartChild();
    }

    protected override void OnStop()
    {
        StopChild();
    }

    protected override void OnShutdown()
    {
        StopChild();
        base.OnShutdown();
    }

    private void StartChild()
    {
        lock (gate)
        {
            if (child != null && !child.HasExited)
                return;
            stopping = false;

            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string commandScript = Path.Combine(baseDirectory, "cxnfs-service.cmd");
            if (!File.Exists(commandScript))
                throw new FileNotFoundException("missing service command script", commandScript);

            string runDirectory = Path.Combine(baseDirectory, "run");
            Directory.CreateDirectory(runDirectory);
            DeleteIfExists(Path.Combine(runDirectory, "server.stop"));
            DeleteIfExists(Path.Combine(runDirectory, "server.ready"));
            DeleteIfExists(Path.Combine(runDirectory, "server.health"));

            string logDirectory = Path.Combine(baseDirectory, "logs");
            Directory.CreateDirectory(logDirectory);

            Process process = new Process();
            process.StartInfo.FileName = Environment.GetEnvironmentVariable("COMSPEC") ?? "cmd.exe";
            process.StartInfo.Arguments = "/d /s /c " + Quote(commandScript);
            process.StartInfo.WorkingDirectory = baseDirectory;
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.CreateNoWindow = true;
            process.EnableRaisingEvents = true;
            process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs eventArgs)
            {
                AppendLine(Path.Combine(logDirectory, "server.out.log"), eventArgs.Data);
            };
            process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs eventArgs)
            {
                AppendLine(Path.Combine(logDirectory, "server.err.log"), eventArgs.Data);
            };
            process.Exited += delegate
            {
                AppendLine(Path.Combine(logDirectory, "service-wrapper.log"),
                    "cxnfs child exited with code " + process.ExitCode);
                if (!stopping)
                    Environment.Exit(process.ExitCode == 0 ? 1 : process.ExitCode);
            };
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            child = process;
        }
    }

    private void StopChild()
    {
        Process process;
        lock (gate)
        {
            process = child;
            child = null;
        }
        if (process == null)
            return;
        stopping = true;
        try
        {
            string stopFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "run", "server.stop");
            Directory.CreateDirectory(Path.GetDirectoryName(stopFile));
            File.WriteAllText(stopFile, "stop" + Environment.NewLine);
        }
        catch
        {
        }
        try
        {
            if (!process.WaitForExit(15000) && !process.HasExited)
                process.Kill();
        }
        catch
        {
        }
        finally
        {
            process.Dispose();
        }
    }

    private static void AppendLine(string path, string line)
    {
        if (line == null)
            return;
        try
        {
            File.AppendAllText(path, line + Environment.NewLine);
        }
        catch
        {
        }
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
        }
    }

    private static string Quote(string value)
    {
        return "\"" + value.Replace("\"", "\\\"") + "\"";
    }
}
