using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CubeMR
{
    public class Doner
    {
        public string DonorID;
        public string DonerState;

    }

    public class Donation
    {
        public string DonorID;
        public double DonationAmount;
    }

    public class AggregateDonation
    {
        public string DonorState;
        public double TotalAmount;
    }

    public class MasterSolver
    {
        protected static int _MaxWorkers = 5;
        protected static int _DonersPerWorker = 1000000;
        protected static int _DonationsPerWorker = 1000000; //TODO improve

        protected string _csvDonationsPath;
        protected string _csvDonorsPath;
        protected string _outputFile;


        protected List<string> _DonnerCutoffValue = new List<string>();
        protected List<string> _DonerSplitsPaths = new List<string>();

        protected List<string> _DonationFileChunkPaths = new List<string>();
        protected List<string> _DonationDonerFilePaths = new List<string>();

        protected List<WorkerMachine> _WorkerMachines = new List<WorkerMachine>();

        protected ConcurrentDictionary<string, double> _DonationsByState = new ConcurrentDictionary<string, double>();

        public MasterSolver(string csvDonorsPath, string csvDonationsPath, string outputFile)
        {
            _csvDonorsPath = csvDonorsPath;
            _csvDonationsPath = csvDonationsPath;
            _outputFile = outputFile;
        }

        /// <summary>
        /// This method assumes the input Doners file is sorted.
        /// </summary>
        protected void SplitDonersFile()
        {
            var donerParser = new Parser(new IOHelper(_csvDonorsPath, null, Encoding.ASCII)); //TODO: Encoding

            var curDonerIdx = 0;

            string nextDonerFileName = getNextDonerFileName();

            Parser donerWriterParser = new Parser(new IOHelper(null, nextDonerFileName, Encoding.ASCII));
            _DonerSplitsPaths.Add(nextDonerFileName);

            while (!donerParser.Done)
            {
                var curDoner = donerParser.ReadDonerLine();
                if (curDoner == null)
                    break;
                curDonerIdx++;

                if (curDonerIdx >= _DonersPerWorker)
                {
                    _DonnerCutoffValue.Add(curDoner.DonorID);
                    curDonerIdx = 0;
                    nextDonerFileName = getNextDonerFileName();
                    donerWriterParser.Dispose();
                    donerWriterParser = new Parser(new IOHelper(null, nextDonerFileName, Encoding.ASCII));
                    _DonerSplitsPaths.Add(nextDonerFileName);
                }

                donerWriterParser.WriteDonorLine(curDoner);
            }

            donerWriterParser.Dispose();
        }

        protected void SplitDonationsFile()
        {
            List<Parser> donationsFileChunks = new List<Parser>();
            List<int> donationsInFileChunk = new List<int>();

            try
            {
                int i;
                for (i = 0; i < _DonerSplitsPaths.Count; i++)
                {
                    var nextDonationFile = getNextDonationFilename();
                    _DonationFileChunkPaths.Add(nextDonationFile);
                    donationsFileChunks.Add(new Parser(new IOHelper(null, nextDonationFile, Encoding.ASCII)));
                    donationsInFileChunk.Add(0);
                    _DonationDonerFilePaths.Add(_DonerSplitsPaths[i]);
                }

                var donationsParser = new Parser(new IOHelper(_csvDonationsPath, null, Encoding.ASCII)); //TODO:Encoding.

                while (!donationsParser.Done)
                {
                    var curDonation = donationsParser.ReadDonationLine();
                    if (curDonation == null)
                        break;
                    var curDonationTargetFileIdx = _DonnerCutoffValue.BinarySearch(curDonation.DonorID);
                    if (curDonationTargetFileIdx < 0)
                    {
                        curDonationTargetFileIdx = ~curDonationTargetFileIdx;
                    }
                    else
                        curDonationTargetFileIdx++;

                    donationsFileChunks[curDonationTargetFileIdx].WriteDonationLine(curDonation);
                    donationsInFileChunk[curDonationTargetFileIdx]++;
                    if (donationsInFileChunk[curDonationTargetFileIdx] >= _DonationsPerWorker)
                    {
                        donationsInFileChunk[curDonationTargetFileIdx] = 0;
                        donationsFileChunks[curDonationTargetFileIdx].Dispose();
                        var nextDonationFile = getNextDonationFilename();
                        donationsFileChunks[curDonationTargetFileIdx] = new Parser(new IOHelper(null, nextDonationFile, Encoding.ASCII));
                        _DonationFileChunkPaths.Add(nextDonationFile);
                        _DonationDonerFilePaths.Add(_DonerSplitsPaths[curDonationTargetFileIdx]);
                    }
                }
            }
            finally
            {
                foreach (var parser in donationsFileChunks)
                {
                    parser.Dispose();
                }
            }
        }

        protected void AggregateResultFile(int machineID, string resultFileToAggregate)
        {
            GetFileFromMachine(resultFileToAggregate, machineID);
            using (var parserAggDonation = new Parser(new IOHelper(resultFileToAggregate, null, Encoding.ASCII)))
            {
                while (!parserAggDonation.Done)
                {
                    var aggDonation = parserAggDonation.ReadAggreagateDonationLine();
                    if (aggDonation == null)
                        break;

                    _DonationsByState.AddOrUpdate(aggDonation.DonorState, aggDonation.TotalAmount, (id, val1) => val1+aggDonation.TotalAmount);
                }
            }
        }

        protected void OutputResults()
        {
            using(var parserAggDonation = new Parser(new IOHelper(null, _outputFile, Encoding.ASCII)))
            {
                foreach(var kvpDonations in _DonationsByState)
                {
                    parserAggDonation.WriteAggregateDonationLine(new AggregateDonation() { DonorState = kvpDonations.Key, TotalAmount = kvpDonations.Value });
                }
            }
        }


        protected int _NextMachineToRun = -1;
        protected void StartupMachines()
        {
            int curMachineIdx = 0;
            for (curMachineIdx = 0; curMachineIdx < _MaxWorkers; curMachineIdx++)
            {
                if (StartupNewMachine() == false)
                    break;
            }
        }

        protected bool StartupNewMachine()
        {
            var machineIdx = Interlocked.Increment(ref _NextMachineToRun);
            if (machineIdx >= _DonationFileChunkPaths.Count)
                return false;

            WorkerMachine wm = new WorkerMachine(machineIdx, _DonationDonerFilePaths[machineIdx], _DonationFileChunkPaths[machineIdx], new Action<int, string>(WorkerCallback));
            SendFileToMachine(_DonationDonerFilePaths[machineIdx], machineIdx);
            SendFileToMachine(_DonationFileChunkPaths[machineIdx], machineIdx);
            wm.Start();

            return true;
        }

        protected void WorkerCallback(int machineID, string workerOutputFile)
        {
            Task aggregateResultTask = new Task(new Action(() => AggregateResultFile(machineID, workerOutputFile)));
            aggregateResultTask.ContinueWith(task => AggregationFinished());
            StartupNewMachine();
            aggregateResultTask.Start();
        }

        protected int _NoFinishedAggregations = 0;

        ManualResetEvent _workDone = new ManualResetEvent(false);

        protected void AggregationFinished()
        {
            var finishedAggregations = Interlocked.Increment(ref _NoFinishedAggregations);
            if (_NoFinishedAggregations >= _DonationDonerFilePaths.Count)
                _workDone.Set();
        }

        protected void WaitForAllMachinesToFinish()
        {
            _workDone.WaitOne();
        }

        public void DoWork()
        {
            try
            {
                Console.WriteLine("Splitting Donors file...");
                SplitDonersFile();
                Console.WriteLine("Splitting Donations file...");
                SplitDonationsFile();
                Console.WriteLine("Firing up initial workers...");
                StartupMachines();
                Console.WriteLine("Waiting for all workers to finish and be aggregated...");
                WaitForAllMachinesToFinish();
                Console.WriteLine("Outputting results...");
                OutputResults();
                Console.WriteLine("DONE.");
            }
            catch (InvalidCastException ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }


        int _nextDonerFNIdx = 0;
        string getNextDonerFileName()
        {
            return $"doner{_nextDonerFNIdx++}.csv";
        }

        int _nextDonationFNIdx = 0;
        string getNextDonationFilename()
        {
            return $"donation{_nextDonationFNIdx++}.csv";
        }

        void SendFileToMachine(string filenameToSend, int machineID)
        {
            //Do nothing since this is POC working on local machine.
        }

        void GetFileFromMachine(string filenameToGet, int machineID)
        {
            //Do nothing since this is POC working on local machine
        }
    }
    public class DonationAggregator
    {
        protected string _csvDonationsPath;
        protected string _csvDonorsPath;
        protected string _outputFile;

        protected Dictionary<string, string> _DonerToState = new Dictionary<string, string>();
        protected Dictionary<string, double> _AggregateDonations = new Dictionary<string, double>();

        public DonationAggregator(string csvDonorsPath, string csvDonationsPath, string outputFile)
        {
            _csvDonorsPath = csvDonorsPath;
            _csvDonationsPath = csvDonationsPath;
            _outputFile = outputFile;
        }

        protected void ReadAllDoners()
        {
            using (var donerParser = new Parser(new IOHelper(_csvDonorsPath, null, Encoding.ASCII)))
            {
                while (!donerParser.Done)
                {
                    var curDoner = donerParser.ReadDonerLine();
                    _DonerToState[curDoner.DonorID] = curDoner.DonerState;
                }
            }
        }

        protected void ProcessDonations()
        {
            using (var donationParser = new Parser(new IOHelper(_csvDonationsPath, null, Encoding.ASCII)))
            {
                while (!donationParser.Done)
                {
                    var curDonation = donationParser.ReadDonationLine();
                    if (curDonation == null)
                        break;
                    var donerState = _DonerToState[curDonation.DonorID]; //Throws exception if not found

                    if (!_AggregateDonations.ContainsKey(donerState))
                        _AggregateDonations.Add(donerState, 0);

                    _AggregateDonations[donerState] += curDonation.DonationAmount;
                }
            }
        }

        protected void OutputResults()
        {
            using (var parserAggDonation = new Parser(new IOHelper(null, _outputFile, Encoding.ASCII)))
            {
                foreach (var kvpDonations in _AggregateDonations)
                {
                    parserAggDonation.WriteAggregateDonationLine(new AggregateDonation() { DonorState = kvpDonations.Key, TotalAmount = kvpDonations.Value });
                }
            }
        }

        public void DoWork()
        {
            try
            {
                ReadAllDoners();
                ProcessDonations();
                OutputResults();
            }
            catch (InvalidCastException ex)
            {
                Console.WriteLine($"Error processing file {_csvDonationsPath}: {ex.ToString()}");
            }
        }
    }

    public class WorkerMachine
    {
        protected Action<int, string> _callback;
        public int MachineID { get; private set; }

        public WorkerMachine(int machineID, string csvDonorsPath, string csvDonationPath, Action<int, string> callback)
        {
            MachineID = machineID;
            _callback = callback;
            _donationAggregator = new DonationAggregator(csvDonorsPath, csvDonationPath, getOutputFileName());
            _workTask = new Task(new Action(() => _donationAggregator.DoWork()));
            _workTask.ContinueWith(new Action<Task>((task) => _callback(MachineID, getOutputFileName())));
        }

        public void Start()
        {
            _workTask.Start();
        }

        string getOutputFileName()
        {
            return $"presult{MachineID}.csv";
        }

        private DonationAggregator _donationAggregator;
        private Task _workTask;
    }

    class Program
    {
        protected static MasterSolver _MasterSolver;

        static void Main(string[] args)
        {
            string donorInputFile = args.Length > 0 ? args[0] : "Donors.csv";
            string donationsInputFile = args.Length > 1 ? args[1] : "Donations.csv";
            string resultFile = args.Length > 2 ? args[2] : "result.csv";
            _MasterSolver = new MasterSolver(donorInputFile, donationsInputFile, resultFile);
            _MasterSolver.DoWork();
        }
    }


    class Parser : IDisposable
    {

        protected IOHelper _ioHelper;

        public Parser(IOHelper iOHelper)
        {
            _ioHelper = iOHelper;
        }

        public bool Done
        {
            get
            {
                return _ioHelper.ReaderDone;
            }
        }

        protected bool _SkippedFirstLine = false;

        public Doner ReadDonerLine()
        {
            if (!_SkippedFirstLine)
            {
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _SkippedFirstLine = true;
            }

            var donerId = _ioHelper.ReadNextToken();
            if (donerId == null)
                return null;
            _ioHelper.ReadNextToken();
            var donerState = _ioHelper.ReadNextToken();
            _ioHelper.ReadNextToken();
            _ioHelper.ReadNextToken();

            return new Doner() { DonorID = donerId, DonerState = donerState };

        }

        public Donation ReadDonationLine()
        {
            if (!_SkippedFirstLine)
            {
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _SkippedFirstLine = true;
            }

            if (_ioHelper.ReadNextToken() == null)
                return null;
            _ioHelper.ReadNextToken();
            var donerId = _ioHelper.ReadNextToken();
            _ioHelper.ReadNextToken();
            var donationAmount = _ioHelper.ReadNextDouble() ?? 0.0;
            _ioHelper.ReadNextToken();

            return new Donation() { DonorID = donerId, DonationAmount = donationAmount };
        }

        public AggregateDonation ReadAggreagateDonationLine()
        {
            if (!_SkippedFirstLine)
            {
                _ioHelper.ReadNextToken();
                _ioHelper.ReadNextToken();
                _SkippedFirstLine = true;
            }
            var donationState = _ioHelper.ReadNextToken();
            if (donationState == null)
                return null;
            var donationAmount = _ioHelper.ReadNextDouble() ?? 0.0;

            return new AggregateDonation() { DonorState = donationState, TotalAmount = donationAmount };
        }

        protected bool _WroteFirstLine = false;

        public void WriteAggregateDonationLine(AggregateDonation aggDonationToWrite)
        {
            if (!_WroteFirstLine)
            {
                _ioHelper.WriteLine("State, Total Donation Amount");
                _WroteFirstLine = true;
            }

            _ioHelper.WriteLine($"{aggDonationToWrite.DonorState},{aggDonationToWrite.TotalAmount.ToString("F2", CultureInfo.InvariantCulture)}");
        }

        public void WriteDonationLine(Donation donationToWrite)
        {
            if (!_WroteFirstLine)
            {
                _ioHelper.WriteLine("Project ID,Donation ID,Donor ID,Donation Included Optional Donation,Donation Amount,Donor Cart Sequence");
                _WroteFirstLine = true;
            }
            _ioHelper.WriteLine($",,{donationToWrite.DonorID},,{donationToWrite.DonationAmount.ToString("F2", CultureInfo.InvariantCulture)},");
        }

        public void WriteDonorLine(Doner donerToWrite)
        {
            if (!_WroteFirstLine)
            {
                _ioHelper.WriteLine("Donor ID,Donor City,Donor State,Donor Is Teacher,Donor Zip");
                _WroteFirstLine = true;
            }
            _ioHelper.WriteLine($"{donerToWrite.DonorID},,{donerToWrite.DonerState},,");
        }

        public void Dispose()
        {
            _ioHelper.Dispose();
        }
    }

    class IOHelper : IDisposable
    {
        StreamReader reader;
        StreamWriter writer;

        public IOHelper(string inputFile, string outputFile, Encoding encoding)
        {
            StreamReader iReader;
            StreamWriter oWriter;
            if (inputFile == null)
                iReader = new StreamReader(Console.OpenStandardInput(), encoding);
            else
                iReader = new StreamReader(inputFile, encoding);

            if (outputFile == null)
                oWriter = new StreamWriter(Console.OpenStandardOutput(), encoding);
            else
            {
                if(!File.Exists(outputFile))
                {
                    File.Create(outputFile).Dispose();
                }
                oWriter = new StreamWriter(outputFile, false, encoding);
            }

            reader = iReader;
            writer = oWriter;

            curLine = new string[] { };
            curTokenIdx = 0;
        }


        string[] curLine;
        int curTokenIdx;

        char[] whiteSpaces = new char[] { ',', '\t', '\r', '\n' };

        public string ReadNextToken()
        {
            if (curTokenIdx >= curLine.Length)
            {
                //Read next line
                string line = reader.ReadLine();
                line = line?.Trim();
                if (line == "")
                {
                    line = reader.ReadLine();
                    line = line?.Trim();
                }
                if (line != null)
                    curLine = line.Split(whiteSpaces, StringSplitOptions.None);
                else
                    curLine = new string[] { };
                curTokenIdx = 0;
            }

            if (curTokenIdx >= curLine.Length)
                return null;

            return curLine[curTokenIdx++];
        }

        public int ReadNextInt()
        {
            return int.Parse(ReadNextToken());
        }

        public double? ReadNextDouble()
        {
            var nextToken = ReadNextToken();
            var result = 0.0;
            if (nextToken == null || nextToken == "")
                return null;
            result = double.Parse(nextToken, CultureInfo.InvariantCulture);
            return result;
        }

        public void Write(string stringToWrite)
        {
            writer.Write(stringToWrite);
        }

        public void WriteLine(string stringToWrite)
        {
            writer.WriteLine(stringToWrite);
        }

        public void WriteLine(double valueToWrite)
        {
            writer.WriteLine(valueToWrite.ToString("F8"));
        }

        public void Dispose()
        {
            try
            {
                if (reader != null)
                {
                    reader.Dispose();
                }
                if (writer != null)
                {
                    writer.Flush();
                    writer.Dispose();
                }
            }
            catch { };
        }


        public void Flush()
        {
            if (writer != null)
            {
                writer.Flush();
            }
        }

        public bool ReaderDone
        {
            get
            {
                return reader.EndOfStream;
            }
        }
    }

}