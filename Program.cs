using System.Text;
using Azure.Identity;
using Azure.Storage.Queues;
using CommandLine;

return await Parser.Default.ParseArguments<AddOptions, SummaryOptions>(args)
    .MapResult(
        (AddOptions opt) => HandleAdd(opt),
        (SummaryOptions opt) => HandleSummary(opt),
        _ => Task.FromResult(1));

static async Task<int> HandleSummary(SummaryOptions options)
{
    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += async (_, e) =>
    {
        await using var stdErr = Console.OpenStandardError();
        await using var stdErrWriter = new StreamWriter(stdErr);
        await stdErrWriter.WriteLineAsync("Stopping... (ctrl-c)");
        await stdErrWriter.FlushAsync();
        cts.Cancel();
        e.Cancel = true;
    };

    var queueUri = new Uri(options.QueueName);
    var queue = new QueueClient(queueUri, new AzureCliCredential());
    var properties = await queue.GetPropertiesAsync(cts.Token);
    Console.WriteLine($"ApproximateMessagesCount: {properties.Value.ApproximateMessagesCount}");
    return 0;
}

static async Task<int> HandleAdd(AddOptions addOptions)
{
    await using var stdErr = Console.OpenStandardError();
    await using var stdErrWriter = new StreamWriter(stdErr);
    await stdErrWriter.WriteLineAsync(addOptions.DestinationQueueName);
    await stdErrWriter.FlushAsync();
    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += async (_, e) =>
    {
        await using var stdErr = Console.OpenStandardError();
        await using var stdErrWriter = new StreamWriter(stdErr);
        await stdErrWriter.WriteLineAsync("Stopping... (ctrl-c)");
        await stdErrWriter.FlushAsync();
        cts.Cancel();
        e.Cancel = true;
    };

    var queueUri = new Uri(addOptions.DestinationQueueName);
    var queue = new QueueClient(
        queueUri,
        new AzureCliCredential(),
        new QueueClientOptions()
        {
            MessageEncoding = QueueMessageEncoding.Base64,
        });
    await queue.CreateIfNotExistsAsync(cancellationToken: cts.Token);

    await using var stdIn = Console.OpenStandardInput();
    using var reader = new StreamReader(stdIn, Encoding.UTF8);
    int count = 0;
    while (true)
    {
        var line = await reader.ReadLineAsync(cancellationToken: cts.Token);
        if (line is null)
        {
            return 0;
        }

        var visibilityTimeout =
            addOptions.VisibilityTimeout is {} visibilityTimeoutOptions
            ? (TimeSpan?)TimeSpan.FromSeconds(count * visibilityTimeoutOptions)
            : null;

        await queue.SendMessageAsync(line, visibilityTimeout: visibilityTimeout);

        count++;
    }
}

[Verb("summary", HelpText = "Get summary information about a storage queue.")]
class SummaryOptions
{
    public SummaryOptions(string queueName)
    {
        QueueName = queueName;
    }

    [Value(0, MetaName = "queueName", HelpText = "The name of the queue to get summary for.", Required = true)]
    public string QueueName { get; }
}

[Verb("add", HelpText = "Adds messages from stdin to a storage queue.")]
class AddOptions
{
    public AddOptions(
        string destinationQueueName,
        double? visibilityTimeout)
    {
        DestinationQueueName = destinationQueueName;
        VisibilityTimeout = visibilityTimeout;
    }

    [Value(0, MetaName = "queueName", HelpText = "The name of the queue to which the messages will be added.", Required = true)]
    public string DestinationQueueName { get; }

    [Option('t', "visibility-timeout-factor", HelpText = "This parameter decided the visibilityTimeout that is set to each messages passed from stdin, visibilityTimeout = visibility-timeout-factor * msgCount (in seconds).", Required = false)]
    public double? VisibilityTimeout { get; }
}
