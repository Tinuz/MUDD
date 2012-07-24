using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Security.Cryptography;

namespace MUDD2012.Producers
{
    public enum HashType{
        MD5,
        SHA1,
        SHA256
    }

    public class SingleFilesProducer
    {

        private readonly SingleFiles _singlefiles = null;
        readonly FileFilter _filter = null;
        private ITargetBlock<FileInformation> _target = null;
        private Int32 _totalItems = 0;
        private Int32 _totalToProcess = 0;
        private Int32 _totalSkipped = 0;

        /// <summary>Gets a source for activity information published by the producer.</summary>
        public ISourceBlock<Tuple<int, int, int>> ActivityLog { get { return m_activityLog; } }
        /// <summary>A buffer of activity information published by the producer.</summary>
        private BroadcastBlock<Tuple<int, int, int>> m_activityLog = new BroadcastBlock<Tuple<int, int, int>>(_ => _);

        private bool _isdone = false;
        public bool Done { get { return _isdone; } }

        private bool _onhold = false;
        public bool Hold { set { this._onhold = value; } }

        //Here is the once-per-class call to initialize the log object
        private readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public SingleFilesProducer(SingleFiles singlefiles, ITargetBlock<FileInformation> target)
        {
            this._singlefiles = singlefiles;
            this._target = target;
            this._filter = new FileFilter();
        }

        public async Task<bool> Produce(CancellationToken cltoken) 
        {
            var result = await TaskEx.RunEx<bool>(async () => {
                var location = new DirectoryInfo(_singlefiles.Path);
                var files = location.EnumerateFiles("*.*", SearchOption.AllDirectories);
                _totalItems = files.Count();
                foreach (var file in files) 
                {
                    try
                    {
                        FileType type = _filter.getFileType(file.Name);
                        if (type == FileType.Text) 
                        {
                            _totalToProcess++;
                            _target.Post(await GetTextFile(file));
                        }
                        else if (type == FileType.Archive) 
                        {
                            _totalItems--;
                            _target.Post(await GetArchiveFile(file));
                        }
                        else if (type == FileType.Email)
                        {
                            _totalItems--;
                            _target.Post(await GetMailFile(file));
                        }
                        else if (type == FileType.Garbage)
                        {
                            _totalSkipped++;
                            //_target.Post(await GetGarbageFile(file));
                        }
                        else if (type == FileType.Unknown)
                        {
                            _totalSkipped++;
                            //_target.Post(await GetArchiveFile(file));
                        }
                        else if (type == FileType.Image)
                        {
                            _totalSkipped++;
                            //_target.Post(await GetArchiveFile(file));
                        }
                        else {
                            _totalSkipped++;
                        }
                    }
                    catch (OperationCanceledException ex)
                    {
                        _isdone = false;
                        _log.Info(String.Format("{0}: Operation canceled by the user!", DateTime.Now));
                    }
                    catch (Exception ex)
                    {
                        _log.Error(String.Format("{0}: {1}", DateTime.Now, ex.Message));
                        continue;
                    }
                }
                m_activityLog.Post(new Tuple<int, int, int>(_totalToProcess, _totalSkipped, _totalItems));
                _target.Complete();
                _isdone = true;
                return _isdone;
            },cltoken);
            return result;
        }

        private async Task<FileInformation> GetTextFile(FileInfo currFile) 
        {
            var result = await TaskEx.RunEx<FileInformation>(async () => {
                FileInformation fileinfo = new FileInformation() { 
                    Id = Guid.NewGuid().ToString("N"),
                    FileName = currFile.Name, 
                    Extension = _filter.getExtension(currFile.Name), 
                    NameOnDisk = currFile.FullName, 
                    FileType = FileType.Text, 
                    FileSize = currFile.Length, 
                    LastAccessTime = currFile.LastAccessTime, 
                    LastWriteTime = currFile.LastWriteTime, 
                    CreationTime = currFile.CreationTime, 
                    FileOrigin = FileOrigin.Disk, 
                    FileStatus = FileStatus.Allocated,
                    MD5Hash = await GetHash(currFile.FullName, HashType.MD5),
                    IsPhysical = true };
                return fileinfo;
            });
            return result;
        }
        private async Task<FileInformation> GetArchiveFile(FileInfo currFile) {
            var result = await TaskEx.RunEx<FileInformation>(async () =>
            {
                FileInformation fileinfo = new FileInformation()
                {
                    Id = Guid.NewGuid().ToString("N"),
                    FileName = currFile.Name,
                    Extension = _filter.getExtension(currFile.Name),
                    NameOnDisk = currFile.FullName,
                    FileType = FileType.Archive,
                    FileSize = currFile.Length,
                    LastAccessTime = currFile.LastAccessTime,
                    LastWriteTime = currFile.LastWriteTime,
                    CreationTime = currFile.CreationTime,
                    FileOrigin = FileOrigin.Disk,
                    FileStatus = FileStatus.Allocated,
                    MD5Hash = await GetHash(currFile.FullName, HashType.MD5),
                    IsPhysical = true
                };
                return fileinfo;
            });
            return result;
        }
        private async Task<FileInformation> GetMailFile(FileInfo currFile) {
            var result = await TaskEx.RunEx<FileInformation>(async () =>
            {
                FileInformation fileinfo = new FileInformation()
                {
                    Id = Guid.NewGuid().ToString("N"),
                    FileName = currFile.Name,
                    Extension = _filter.getExtension(currFile.Name),
                    NameOnDisk = currFile.FullName,
                    FileType = FileType.Email,
                    FileSize = currFile.Length,
                    LastAccessTime = currFile.LastAccessTime,
                    LastWriteTime = currFile.LastWriteTime,
                    CreationTime = currFile.CreationTime,
                    FileOrigin = FileOrigin.Disk,
                    FileStatus = FileStatus.Allocated,
                    MD5Hash = await GetHash(currFile.FullName, HashType.MD5),
                    IsPhysical = true
                };
                return fileinfo;
            });
            return result;
        }
        private async Task<FileInformation> GetUnknownFile(FileInfo currFile) {
            var result = await TaskEx.Run<FileInformation>(() =>
            {
                FileInformation fileinfo = new FileInformation()
                {
                    FileName = currFile.Name,
                    Extension = _filter.getExtension(currFile.Name),
                    NameOnDisk = currFile.FullName,
                    FileType = FileType.Unknown,
                    FileSize = currFile.Length,
                    LastAccessTime = currFile.LastAccessTime,
                    LastWriteTime = currFile.LastWriteTime,
                    CreationTime = currFile.CreationTime,
                    FileOrigin = FileOrigin.Disk,
                    FileStatus = FileStatus.Allocated,
                    IsPhysical = true
                };
                return fileinfo;
            });
            return result;
        }
        private async Task<FileInformation> GetGarbageFile(FileInfo currFile) {
            var result = await TaskEx.Run<FileInformation>(() =>
            {
                FileInformation fileinfo = new FileInformation()
                {
                    FileName = currFile.Name,
                    Extension = _filter.getExtension(currFile.Name),
                    NameOnDisk = currFile.FullName,
                    FileType = FileType.Garbage,
                    FileSize = currFile.Length,
                    LastAccessTime = currFile.LastAccessTime,
                    LastWriteTime = currFile.LastWriteTime,
                    CreationTime = currFile.CreationTime,
                    FileOrigin = FileOrigin.Disk,
                    FileStatus = FileStatus.Allocated,
                    IsPhysical = true
                };
                return fileinfo;
            });
            return result;
        }
        private async Task<FileInformation> GetImageFile(FileInfo currFile) {
            var result = await TaskEx.Run<FileInformation>(() =>
            {
                FileInformation fileinfo = new FileInformation()
                {
                    FileName = currFile.Name,
                    Extension = _filter.getExtension(currFile.Name),
                    NameOnDisk = currFile.FullName,
                    FileType = FileType.Image,
                    FileSize = currFile.Length,
                    LastAccessTime = currFile.LastAccessTime,
                    LastWriteTime = currFile.LastWriteTime,
                    CreationTime = currFile.CreationTime,
                    FileOrigin = FileOrigin.Disk,
                    FileStatus = FileStatus.Allocated,
                    IsPhysical = true
                };
                return fileinfo;
            });
            return result;
        }
        private async Task<string> GetHash(string path, HashType hash) {
            var result = await TaskEx.Run<string>(() => {
                string _hash = string.Empty;
                if (hash == HashType.MD5)
                {
                    using (MD5 md5 = new MD5CryptoServiceProvider())
                    {
                        using (FileStream file = new FileStream(path, FileMode.Open))
                        {
                            byte[] retVal = md5.ComputeHash(file);
                            _hash = BitConverter.ToString(retVal).Replace("-", "");
                        }
                    }
                }
                else if (hash == HashType.SHA1)
                {
                    using (SHA1 sha1 = new SHA1CryptoServiceProvider())
                    {
                        using (FileStream file = new FileStream(path, FileMode.Open))
                        {
                            byte[] retVal = sha1.ComputeHash(file);
                            _hash = BitConverter.ToString(retVal).Replace("-", "");
                        }
                    }
                }
                else if (hash == HashType.SHA256) 
                {
                    using (SHA256 sha256 = new SHA256CryptoServiceProvider())
                    {
                        using (FileStream file = new FileStream(path, FileMode.Open))
                        {
                            byte[] retVal = sha256.ComputeHash(file);
                            _hash = BitConverter.ToString(retVal).Replace("-", "");
                        }
                    }
                }
                else
                {
                    using (MD5 md5 = new MD5CryptoServiceProvider())
                    {
                        using (FileStream file = new FileStream(path, FileMode.Open))
                        {
                            byte[] retVal = md5.ComputeHash(file);
                            _hash = BitConverter.ToString(retVal).Replace("-", "");
                        }
                    }
                }
                return _hash;
            });
            return result;
        }
    }
}
