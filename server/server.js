#!/usr/bin/env node
const fs = require('fs');
const net = require('net');
const path = require('path');
const readline = require('readline');
const node_statvfs = require('node-statvfs'); // npm install node-statvfs

// provide all filesystem functionality through a network socket, on the target location

async function get_filehandle(_path, fd, createIfNotExists)
{
    if(fd === 0)
    {
        fh = this.fileHandles[_path];
        
        if(createIfNotExists && !fh)
        {
            // open handle on the fly
            fh = this.fileHandles[_path] = {
                fileHandle: await fs.promises.open(path.join(this.targetDirectory, _path), 'a+')
                // we could mark here that the filehandle is opened by some remote client
            };
        }
    }
    else
    {
        fh = this.fileHandles[fd];
    }
    
    if(!fh) throw {code: 'EBADFD'};
    
    return fh;
}

const command_map = {
    access: async function(_path, mode)
    {
        // see: https://man7.org/linux/man-pages/man2/access.2.html
        // mode is one of F_OK, R_OK, W_OK, X_OK
        
        await fs.promises.access(path.join(this.targetDirectory, _path), mode);
    },
    statfs: function(_path)
    {
        const dir = path.join(this.targetDirectory, _path);
        
        // returns block info: frsize,bsize; byte count/free: bsize,bavail; inode count/free: files,favail
        return new Promise(function(resolve, reject)
        {
            node_statvfs.statvfs(dir, function(err, stats)
            {
                if(err)
                {
                    reject(err);
                }
                else
                {
                    // also supply without f_ prefix
                    for(var k in stats)
                    {
                        if(k.startsWith('f_'))
                        {
                            stats[k.substring(2)] = stats[k];
                        }
                    }
                    
                    resolve(stats);
                }
            });
        });
    },
    getattr: function(_path)
    {
        return fs.promises.stat(path.join(this.targetDirectory, _path));
    },
    fgetattr: async function(_path, fd)
    {
        var fh = await get_filehandle.call(this, _path, fd);
        
        return await fh.fileHandle.stat();
    },
    create: async function(_path, mode)
    {
        // https://man7.org/linux/man-pages/man3/creat.3p.html
        
        // extract the last four octal digits
        var permissions = mode % (8**4);
        
        // the equivalent, in more verbose with string manipulation:
        // var octal_digits = Number(mode).toString(8);
        // var permissions = parseInt(octal_digits.substring(octal_digits.length - 4), 8);
        
        const fh = await fs.promises.open(path.join(this.targetDirectory, _path), 'w', permissions);
        
        fh.close();
    },
    open: async function(_path, flags)
    {
        var fh = await fs.promises.open(path.join(this.targetDirectory, _path), flags);
        this.fileHandles[fh.fd] = {
            fileHandle: fh
            // we could mark here that the filehandle is opened by some remote client
        };
        return fh.fd;
    },
    read: async function(_path, fd, buf_encoding, len, pos)
    {
        if(buf_encoding === 'base64')
        {
            var fh = await get_filehandle.call(this, _path, fd);
            
            var buffer = Buffer.allocUnsafe(len);
            var res = await fh.fileHandle.read(buffer, 0, len, pos);
            
            // either bytesRead should equal the intended length, or less were actually read because an EOF was encountered (for instance when reading 4096 buffer bytes)
            if(res.bytesRead === len || res.bytesRead > 0)
            {
                return buffer.subarray(0, res.bytesRead).toString('base64');
            }
            else
            {
                throw {code: 'EIO'};
            }
        }
        else
        {
            throw {code: 'EINVAL'};
        }
    },
    write: async function(_path, fd, buf, len, pos, buf_encoding)
    {
        var fh = await get_filehandle.call(this, _path, fd, true);
        
        var res = await fh.fileHandle.write(Buffer.from(buf, buf_encoding || 'base64'), 0, len, pos);
        
        return res.bytesWritten;
    },
    fsync: async function(_path, fd, datasync)
    {
        var fh = await get_filehandle.call(this, _path, fd);
        
        if(datasync)
        {
            fh.datasync();
        }
        else
        {
            fh.sync();
        }
        
        return 0;
    },
    ftruncate: async function(_path, fd, size)
    {
        var fh = await get_filehandle.call(this, _path, fd, false);
        
        await fh.fileHandle.truncate(size);
    },
    release: async function(_path, fd)
    {
        try
        {
            var fh = await get_filehandle.call(this, _path, fd);
            
            // first delete (before await), since this file descriptor/handle is now closing...
            delete this.fileHandles[k];
            
            await fh.fileHandle.close();
        }
        catch(err)
        {
            // ignore close error
        }
    },
    // mkfifo: function(_path, mode)
    // {
        // note: in order to read from a unix FIFO, we should use:
        // fh=fs.promises.open('/path/to/fifo', fs.constants.O_RDONLY | fs.constants.O_NONBLOCK)
        // pipe = new net.Socket({fd: fh.fd})
        // pipe.on('data', chunk => ...)
        // because the fs module does not work with non-blocking mode, but the net.Socket can do it
        // implementing support for mkfifo, also means to change the read-function, which needs to know the type (so during open, the type must be known)
    // },
    mknod: function(_path, mode, dev)
    {
        // see: https://man7.org/linux/man-pages/man7/inode.7.html
        var filetype = mode & 0o170000;
        // var permissions = mode & 0o7777; // permissions includes special sticky/set-user-ID/set-group-ID bit as the first digit (MSB)
        
        if(filetype === 0o100000) // S_IFREG
        {
            // do regular file
            return command_map.create(_path, mode);
        }
        else if(filetype === 0o040000) // S_IFDIR
        {
            // do directory
            return command_map.mkdir(_path, mode);
        }
        else
        {
            throw {code: 'EOPNOTSUPP'};
            
            // unsupported filetype:
            // "The only portable use of mknod() is to create a FIFO-special file. If mode is not S_IFIFO or dev is not 0, the behavior of mknod() is unspecified."
            // However, mkfifo(3) should be used instead, therefore mknod is never really needed.
        }
    },
    truncate: async function(_path, size)
    {
        await fs.promises.truncate(path.join(this.targetDirectory, _path), size);
    },
    readlink: async function(_path)
    {
        var destinationPath = await fs.promises.realpath(path.join(this.targetDirectory, _path));
        
        // strip the leading directory path
        if(destinationPath.startsWith(this.targetDirectory))
        {
            destinationPath = destinationPath.substring(this.targetDirectory.length + 1);
            
            return destinationPath;
        }
        else
        {
            // warning: destination path is not located within the target directory (jailbreak), ignore any destination, it does not really exist here
            throw {code: 'EACCES'};
        }
    },
    unlink: async function(_path)
    {
        await fs.promises.unlink(path.join(this.targetDirectory, _path));
    },
    rename: async function(src, dst)
    {
        await fs.promises.rename(path.join(this.targetDirectory, src), path.join(this.targetDirectory, dst));
    },
    mkdir: async function(_path, mode)
    {
        var permissions = mode % (8**4);
        
        await fs.promises.mkdir(path.join(this.targetDirectory, _path), {mode: permissions});
    },
    rmdir: async function(_path)
    {
        await fs.promises.rmdir(path.join(this.targetDirectory, _path));
    },
    readdir: function(_path)
    {
        return fs.promises.readdir(path.join(this.targetDirectory, _path));
    },
    chmod: async function(_path, mode)
    {
        var permissions = mode % (8**4);
        
        await fs.promises.chmod(path.join(this.targetDirectory, _path), permissions);
    },
    chown: async function(_path, uid, gid)
    {
        await fs.promises.chown(path.join(this.targetDirectory, _path), uid, gid);
    },
    utimens: async function(_path, atime, mtime)
    {
        await fs.promises.utimes(path.join(this.targetDirectory, _path), atime * 1000, mtime * 1000);
    },
    symlink: async function(src, dst)
    {
        await fs.promises.symlink(dst.startsWith('/') ? path.join(this.targetDirectory, dst) : dst, path.join(this.targetDirectory, src));
    }
};

async function handle(request, response)
{
    const context = this;
    const locks = context.locks;
    
    // YetiFS transaction: FUSE request --> lock[A,B,C,...] --> exec[X,Y] --> unlock[A,B,C,...] --> FUSE callback
    //                                                      --> Broken connection: unlock[Q]
    //                                                      --> Node crash: unlock[Q]
    // whenever a connection is lost, all locks are considered unlocked as well
    // so a connection may only be opened to a new node, if the path through the connection was previously already locked
    
    if(request.action === 'lock')
    {
        if(!(request.path in locks))
        {
            // initialize a new lock
            locks[request.path] = {
                promise: new Promise(resolve => resolve()), // create an immediately fulfilled promise
                unlock: null,
                resolved: true
            };
        }
        
        // set default timeout to 60s, if any single operation lasts longer than this timeout, then give up
        if(typeof request.timeoutMS !== 'number') request.timeoutMS = 60000;
        
        function apply_lock(lock, cb)
        {
            return lock.promise = new Promise(function(resolve, reject)
            {
                lock.unlock = function()
                {
                    lock.resolved = true;
                    lock.unlock = null;
                    cb();
                    resolve();
                };
            });
        }
        
        try
        {
            await new Promise((resolve, reject) =>
            {
                // check if socket is still open
                if(request.socket.readyState !== 'open')
                {
                    return reject('ECONNABORTED');
                }
                
                var lock = locks[request.path];
                var ownPromise = null;
                
                // if resolved, then the promise is already resolved, so we can directly assign a new promise
                if(lock.resolved)
                {
                    lock.resolved = false;
                    
                    function cleanup_handler()
                    {
                        if(lock.promise === ownPromise && lock.unlock !== null)
                        {
                            lock.unlock();
                        }
                    }
                    request.socket.on('close', cleanup_handler);
                    
                    ownPromise = apply_lock(lock, () => request.socket.off('close', cleanup_handler));
                    
                    return resolve();
                }
                
                if(request.LOCK_NB === true)
                {
                    // if promise is active, but timeout is zero (or negative), then it is same as if non-blocking flag is set
                    return reject('EWOULDBLOCK');
                }
                
                // setup a timer, for a non-infinite timeout waiting for a lock
                var timer;
                if(request.timeoutMS > 0)
                {
                    timer = setTimeout(() =>
                    {
                        timer = true;
                        reject('ETIME');
                        
                    }, request.timeoutMS);
                }
                
                // if request socket has been closed, then we must also unlock this path:
                function timeout_handler(err)
                {
                    if(timer === true) return; // Promise already handled
                    
                    // abort trying to get a lock
                    clearTimeout(timer);
                    timer = true;
                    reject('ECONNABORTED');
                }
                request.socket.on('close', timeout_handler);
                
                // try to be the first to get the promise, after the current one is resolved
                function trylock()
                {
                    if(timer === true) return; // timer already rejected the Promise
                    
                    // try to create the new promise
                    if(lock.resolved)
                    {
                        // we can safely assign our own promise, and mark we own this lock
                        lock.resolved = false;
                        
                        // add new socket close function
                        function cleanup_handler()
                        {
                            if(lock.promise === ownPromise && lock.unlock !== null)
                            {
                                lock.unlock();
                            }
                        }
                        request.socket.on('close', cleanup_handler);
                        
                        // remove previous socket close function
                        request.socket.off('close', timeout_handler);
                        
                        ownPromise = apply_lock(lock, () => request.socket.off('close', cleanup_handler));
                        
                        // lock granted, resolve
                        clearTimeout(timer);
                        timer = true;
                        resolve();
                    }
                    else
                    {
                        // we were too late, the promise is already overwritten, wait again...
                        lock.promise.then(trylock);
                    }
                }
                
                lock.promise.then(trylock);
            });
            
            response.code = 'OK';
        }
        catch(err)
        {
            console.log(err);
            response.code = err || 'EPERM';
        }
    }
    else if(request.action === 'exec')
    {
        // do whatever we gotta do on the local filesystem
        try
        {
            if(!(request.command in command_map))
            {
                response.code = 'EOPNOTSUPP';
            }
            else
            {
                const result = await command_map[request.command].apply(context, request.args);
                
                if(typeof result !== 'undefined')
                {
                    response.result = result;
                }
                
                response.code = 'OK';
            }
        }
        catch(err)
        {
            console.log(err);
            
            response.code = err.code || 'EIO';
        }
    }
    else if(request.action === 'unlock')
    {
        var lock = locks[request.path];
        
        if(lock && typeof lock.unlock === 'function')
        {
            lock.unlock();
            
            response.code = 'OK';
        }
        else
        {
            response.code = 'ENOENT';
        }
    }
    
    return response;
}

async function accept(socket)
{
    // expect line-buffered JSON-objects
    const client_context = {
        locks: this.locks,
        targetDirectory: this.targetDirectory,
        fileHandles: {}
    };
    
    console.log('accepted a client');
    
    var client = readline.createInterface({
        input: socket
    });
    
    try
    {
        for await (const line of client)
        {
            try
            {
                var request = JSON.parse(line);
                console.log(request);
                request.socket = socket;
                
                // request: {socket: ..., action: <lock|exec|unlock>}
                // request: {action: <lock|unlock>, path: '...'}
                // request: {action: <exec>, command: '...', args: ['', '', ...]}
                
                var response = await handle.call(client_context, request, {});
                
                console.log(response);
                
                socket.write(JSON.stringify(response) + '\n');
            }
            catch(err)
            {
                // invalid line or processing error
                console.log('error:', err);
            }
        }
    }
    catch(err)
    {
        console.log(err);
    }
    
    console.log('client disconnected');
    
    // release all filehandles that this client has
    for(var k in client_context.fileHandles)
    {
        try
        {
            // note: if k starts with /, it's a path, otherwise it's a file descriptor number
            
            console.log('cleaning up filehandle: ' + k);
            
            var fh = client_context.fileHandles[k];
            
            delete client_context.fileHandles[k];
            
            await fh.fileHandle.close();
        }
        catch(err)
        {
            // maybe it is already closed
            console.log(err);
        }
    }
}

function server(options)
{
    options = options || {};
    
    var listenPort = options.listenPort || 8081;
    var listenBind = options.listenBind || 'localhost';
    
    const context = {
        locks: {},
        targetDirectory: path.resolve(options.targetDirectory || '.')
    };
    
    const server = net.createServer();
    server.on('listening', function()
    {
        console.log('started listening (' + context.targetDirectory + ')');
    });
    server.on('connection', function(socket)
    {
        accept.call(context, socket);
    });
    server.on('error', function(err)
    {
        console.error(err);
    });
    server.on('close', function()
    {
        console.log('stopped listening');
    });
    server.listen(listenPort, listenBind);
}

if(require.main === module)
{
    const options = {};
    for(var i=2;i<process.argv.length;++i)
    {
        const arg = process.argv[i];
        
        if(arg === '--' || arg.startsWith('-'))
        {
            if(arg === '-p')
            {
                options.listenPort = parseInt(process.argv[++i]);
            }
            else if(arg === '-b')
            {
                options.listenBind = process.argv[++i];
            }
        }
        else
        {
            break;
        }
    }
    
    if(i < process.argv.length)
    {
        options.targetDirectory = process.argv[i++];
    }
    
    server(options);
}
else
{
    module.exports = server;
}
