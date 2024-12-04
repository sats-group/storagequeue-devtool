using System.Runtime.CompilerServices;
using System.Text;
using Azure.Identity;
using Azure.Storage.Queues;
using CommandLine;

return await Parser.Default.ParseArguments<AddOptions, SummaryOptions, DequeueOptions>(args)
    .MapResult(
        (AddOptions opt) => HandleAdd(opt),
        (SummaryOptions opt) => HandleSummary(opt),
        (DequeueOptions opt) => HandleDequeue(opt),
        _ => Task.FromResult(1));

static async Task<int> HandleDequeue(DequeueOptions options)
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
    var response = await queue.ReceiveMessagesAsync(maxMessages: 32, cancellationToken: cts.Token);

    while (response.Value.Length > 0)
    {
        foreach (var peeked in response.Value)
        {
            Console.Write($"{Encoding.UTF8.GetString(Convert.FromBase64String(peeked.Body.ToString()))}");
            Console.Write(options.ColumnSeparator);
            Console.Write(peeked.MessageId);
            Console.Write(options.ColumnSeparator);
            Console.Write(peeked.InsertedOn);
            Console.Write(options.Separator);
            if (!options.PeekOnly)
            {
                await queue.DeleteMessageAsync(peeked.MessageId, peeked.PopReceipt, cancellationToken: cts.Token);
            }
        }

        response = await queue.ReceiveMessagesAsync(maxMessages: 32, cancellationToken: cts.Token);
    }

    return 0;
}

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
    if (addOptions.ColumnSeparator is not null && addOptions.ColumnSelector is null)
    {
        Console.Error.WriteLine("ColumnSelector is necessary when ColumnSeparator is set.");
        return 1;
    }

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
    await foreach (var line in ReadUntil(reader, addOptions.Separator, addOptions.ColumnSeparator, cts.Token))
    {
        var visibilityTimeout =
            addOptions.VisibilityTimeout is {} visibilityTimeoutOptions
            ? (TimeSpan?)TimeSpan.FromSeconds(count * visibilityTimeoutOptions)
            : null;

        var data = line[addOptions.ColumnSelector ?? 0];
        if (addOptions.DryRun)
        {
            Console.WriteLine(data);
        }
        else
        {
            await queue.SendMessageAsync(data, visibilityTimeout: visibilityTimeout);
        }

        count++;
    }
    return 0;
}

static async IAsyncEnumerable<string[]> ReadUntil(StreamReader reader, string separator, string? columnSeparator, [EnumeratorCancellation] CancellationToken cancellationToken)
{
    StringBuilder? current = null;
    while (true)
    {
        var line = await reader.ReadLineAsync(cancellationToken: cancellationToken);
        if (line is null)
        {
            if (current is not null)
            {
                yield return
                    columnSeparator is not null
                    ? current.ToString().Split(columnSeparator)
                    : [current.ToString()];
            }

            break;
        }

        var split = line.Split(separator, 2);
        if (split.Length == 1)
        {
            current ??= new StringBuilder();
            current.Append(split[0]);
            current.Append(Environment.NewLine);

            continue;
        }

        if (!string.IsNullOrEmpty(split[0]))
        {
            current ??= new StringBuilder();
            current.Append(split[0]);
        }

        if (current is not null)
        {
            yield return
                columnSeparator is not null
                ? current.ToString().Split(columnSeparator)
                : [current.ToString()];
        }

        if (string.IsNullOrEmpty(split[1]))
        {
            current = null;
        }
        else
        {
            current = new StringBuilder(split[1]);
            current.Append(Environment.NewLine);
        }
    }
}

[Verb("dequeue", HelpText = "dequeue.")]
class DequeueOptions
{
    [Value(0, MetaName = "queueName", HelpText = "The name of the queue to dequeue.", Required = true)]
    public required string QueueName { get; init; }

    [Option('s', "separator", HelpText = "This parameter decides how to separate messages (default: new line).", Required = false)]
    public string Separator { get; init; } = Environment.NewLine;

    [Option('c', "column-separator", HelpText = "This parameter decides how to separate columns (default: ,).", Required = false)]
    public string ColumnSeparator { get; init; } = ",";

    [Option('p', "peek-only", HelpText = "This parameter decides if the message will be deleted from the queue (default: false).", Required = false)]
    public bool PeekOnly { get; init; }
}

[Verb("summary", HelpText = "dequeue.")]
class SummaryOptions
{
    public SummaryOptions(string queueName)
    {
        QueueName = queueName;
    }

    [Value(0, MetaName = "queueName", HelpText = "The name of the queue to dequeue.", Required = true)]
    public string QueueName { get; }
}

[Verb("add", HelpText = "Adds messages from stdin to a storage queue.")]
class AddOptions
{
    [Value(0, MetaName = "queueName", HelpText = "The name of the queue to which the messages will be added.", Required = true)]
    public required string DestinationQueueName { get; init; }

    [Option('t', "visibility-timeout-factor", HelpText = "This parameter decided the visibilityTimeout that is set to each messages passed from stdin, visibilityTimeout = visibility-timeout-factor * msgCount (in seconds).", Required = false)]
    public double? VisibilityTimeout { get; init; }

    [Option('s', "separator", HelpText = "This parameter decides how to separate messages (default: new line).", Required = false)]
    public string Separator { get; init; } = Environment.NewLine;

    [Option("column-separator", HelpText = "This parameter decides how to separate columns (default is 'null' and the line will not be splitted.).", Required = false)]
    public string? ColumnSeparator { get; init; }

    [Option("column-selector", HelpText = "This parameter decides which columns to send, need if 'column-separator' is _not_ null (default: null).", Required = false)]
    public int? ColumnSelector { get; init; }

    [Option("dry-run", HelpText = "This parameter decides if the message will be added to the queue or just printed to stdout (default: false).", Required = false)]
    public bool DryRun { get; init; }
}
