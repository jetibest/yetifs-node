#!/usr/bin/env node
const fs = require('fs');
const net = require('net');
const path = require('path');
const events = require('events');
const child_process = require('child_process');
const readline = require('readline');
const Fuse = require('fuse-native'); // npm install fuse-native

// FUSE docs: https://libfuse.github.io/doxygen/structfuse__operations.html

// note: to simulate O_APPEND operations, FUSE will do getattr to get the filesize, and then write starting at the retrieved filesize position

// for distributed filesystem, this is a FUSE-mount that connects to a proxy server, that will route the request to the right server (from a pool), based on mtime, amount of freely available space, and parent directories.
// if a file exists, choose server with highest mtime
// if file does not exist, choose server that has most of the directory structure already
// if multiple servers have all directories, then select the server with the most available free disk space

const Mount = {
    bind: function(sourcePath, targetPath)
    {
        return new Promise(function(resolve, reject)
        {
            var p = child_process.spawn('mount', ['-o', 'bind', sourcePath, targetPath], {timeout: 60000});
            
            p.on('close', function(exitCode)
            {
                if(exitCode !== 0)
                {
                    reject(new Error('Non-zero exit code (' + exitCode + ') from child process: mount -o bind "' + sourcePath + '" "' + targetPath + '"'));
                }
                else
                {
                    resolve(true);
                }
            });
        });
    },
    umountSync: function(targetPath)
    {
        var result = child_process.spawnSync('umount', [targetPath], {timeout: 60000});
        
        if(result.error)
        {
            throw result.error;
        }
        else if(result.status !== 0)
        {
            throw new Error('Non-zero exit code (' + result.status + ') from child process: ' + result.stderr.toString('utf8'));
        }
        else
        {
            return true;
        }
    }
};

async function fuseReport(fn, cb)
{
    try
    {
        const response = await fn();
        
        if(response.code === 'OK')
        {
            if('result' in response)
            {
                console.log('Received result:');
                console.log(response.result);
                
                cb(response.value || 0, response.result);
            }
            else
            {
                console.log('Received value: ' + (response.value || 0));
                cb(response.value || 0);
            }
        }
        else
        {
            console.log('Received error response: ' + response.code);
            // report Fuse error code, or fallback to not implemented error (referring to this particular error-code not being implemented)
            cb(Fuse[response.code] || Fuse.ENOSYS);
        }
    }
    catch(err)
    {
        console.log(err);
        // report Fuse error code, or fallback to I/O error
        cb(Fuse[err.code] || Fuse.EIO);
    }
}

var fuseOptionsFn = function()
{
    const context = this;
    const conn_reader = context.reader;
    const conn_writer = context.writer;
    
    return {
        init: async function(cb)
        {
            console.log('FUSE.init()');
            
            cb(0);
        },
        access: async function(_path, mode, cb)
        {
            console.log('FUSE.access(' + _path + ', ' + mode + ')');
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'access',
                    args: [
                        _path, mode
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        utimens: async function(_path, atime, mtime, cb)
        {
            console.log('FUSE.utimens(' + _path + ', ' + atime + ', ' + mtime + ')');
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'utimens',
                    args: [
                        _path, atime, mtime
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        symlink: async function(src, dst, cb)
        {
            console.log('FUSE.symlink(' + src + ', ' + dst + ')');
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'symlink',
                    args: [
                        src, dst
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        create: async function(_path, mode_decimal, cb)
        {
            console.log('FUSE.create(' + _path + ', ' + mode_decimal + ')');
            
            // https://stackoverflow.com/questions/11669504/store-st-mode-from-stat-in-a-file-and-reuse-it-in-chmod
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'create',
                    args: [
                        _path, mode_decimal
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        chmod: async function(_path, mode, cb)
        {
            console.log('FUSE.chmod(' + _path + ', ' + mode + ')');
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'chmod',
                    args: [
                        _path, mode
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        chown: async function(_path, uid, gid, cb)
        {
            console.log('FUSE.chown(' + _path + ', ' + uid + ', ' + gid + ')');
            
            fuseReport(async() =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'chown',
                    args: [
                        _path, uid, gid
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        getattr: async function(_path, cb)
        {
            console.log('FUSE.getattr(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'getattr',
                    args: [
                        _path
                    ]
                });
                
                const response = await conn_reader.read();
                
                if(response.code === 'OK' && response.result)
                {
                    if(typeof response.result.atime === 'string') response.result.atime = new Date(response.result.atime);
                    if(typeof response.result.mtime === 'string') response.result.mtime = new Date(response.result.mtime);
                    if(typeof response.result.ctime === 'string') response.result.ctime = new Date(response.result.ctime);
                    if(typeof response.result.birthtime === 'string') response.result.birthtime = new Date(response.result.birthtime);
                }
                
                return response;
            }, cb);
        },
        fgetattr: async function(_path, fd, cb)
        {
            console.log('FUSE.fgetattr(' + _path + ', ' + fd + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'fgetattr',
                    args: [
                        _path, fd
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        /*flush: async function(_path, fd, cb)
        {
            try
            {
                var fh = memory.fileHandles[fd];
                
                // flush buffers for writing, this is not the same as sync
                // fh.fd;
                
                cb(0);
            }
            catch(err)
            {
                cb(-1);
            }
        },
        fsync: async function(_path, fd, datasync, cb)
        {
            
        },*/
        mknod: async function(_path, mode, dev, cb)
        {
            console.log('FUSE.mknod(' + _path + ', ' + mode + ', ' + dev + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'mknod',
                    args: [
                        _path, mode, dev
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        truncate: async function(_path, size, cb)
        {
            console.log('FUSE.truncate(' + _path + ', ' + size + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'truncate',
                    args: [
                        _path, size
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        ftruncate: async function(_path, fd, size, cb)
        {
            console.log('FUSE.ftruncate(' + _path + ', ' + fd + ', ' + size + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'ftruncate',
                    args: [
                        _path, fd, size
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        readlink: async function(_path, cb)
        {
            console.log('FUSE.readlink(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'readlink',
                    args: [
                        _path
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        open: async function(_path, flags, cb)
        {
            console.log('FUSE.open(' + _path + ', ' + flags + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'open',
                    args: [
                        _path, flags
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        read: async function(_path, fd, buf, len, pos, cb)
        {
            console.log('FUSE.read(' + _path + ', ' + fd + ', ' + buf + ', ' + len + ', ' + pos + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'read',
                    args: [
                        _path, fd, 'base64', len, pos
                    ]
                });
                
                const response = await conn_reader.read();
                
                if(response.code === 'OK')
                {
                    var res_buffer = Buffer.from(response.result, 'base64');
                    
                    response.value = res_buffer.length;
                    delete response.result;
                    
                    res_buffer.copy(buf);
                }
                
                return response;
            }, cb);
        },
        write: async function(_path, fd, buf, len, pos, cb)
        {
            console.log('FUSE.write(' + _path + ', ' + fd + ', ' + buf + ', ' + len + ', ' + pos + ')');
            
            fuseReport(async () =>
            {
                var buf_encoding = 'base64';
                
                await conn_writer.write({
                    action: 'exec',
                    command: 'write',
                    args: [
                        _path, fd, buf.toString(buf_encoding), len, pos, buf_encoding
                    ]
                });
                
                const response = await conn_reader.read();
                
                if(response.code === 'OK')
                {
                    response.value = response.result;
                    delete response.result;
                }
                
                return response;
            }, cb);
        },
        release: async function(_path, fd, cb)
        {
            console.log('FUSE.release(' + _path + ', ' + fd + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'release',
                    args: [
                        _path, fd
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        unlink: async function(_path, cb)
        {
            console.log('FUSE.unlink(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'unlink',
                    args: [
                        _path
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        rename: async function(src, dst, cb)
        {
            console.log('FUSE.rename(' + src + ', ' + dst + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'rename',
                    args: [
                        src, dst
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        mkdir: async function(_path, mode, cb)
        {
            console.log('FUSE.mkdir(' + _path + ', ' + mode + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'mkdir',
                    args: [
                        _path, mode
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        rmdir: async function(_path, cb)
        {
            console.log('FUSE.rmdir(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'rmdir',
                    args: [
                        _path
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        /*opendir: async function(_path, flags, cb)
        {
            var opts = config.options;
            
            try
            {
                var fsDir = await fs.promises.opendir(path.join(opts.localRefDirectory, _path));
                
                var fd = memory.dirHandles.length;
                memory.dirHandles.push(fsDir);
                
                cb(fd);
            }
            catch(err)
            {
                cb(Fuse.ENOENT);
            }
        },*/
        readdir: async function(_path, cb)
        {
            console.log('FUSE.readdir(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'readdir',
                    args: [
                        _path
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        },
        /*releasedir: async function(_path, fd, cb)
        {
            opendir and releasedir are not necessary, what is this even for, there is no stream to read from that can be passed through anyway
        },*/
        statfs: async function(_path, cb)
        {
            console.log('FUSE.statfs(' + _path + ')');
            
            fuseReport(async () =>
            {
                await conn_writer.write({
                    action: 'exec',
                    command: 'statfs',
                    args: [
                        _path
                    ]
                });
                
                return await conn_reader.read();
            }, cb);
        }
    };
};


async function client(options)
{
    var mountPath = options.mountPath || '.';
    var remoteHostname = options.remoteHostname || 'localhost';
    var remotePort = options.remotePort || 8081;
    
    // ensure directory exist:
    try
    {
        await fs.promises.mkdir(mountPath);
    }
    catch(err)
    {
        if(err.code !== 'EEXIST')
        {
            throw err;
        }
    }
    
    // resolve path to absolute path, and follow any symlinks:
    var realMountPath = await fs.promises.realpath(mountPath);
    
    // check if FUSE is configured (and automatically fix if possible):
    try
    {
        await new Promise((resolve, reject) =>
        {
            Fuse.isConfigured((err, bool) =>
            {
                if(err) return reject(err);
                if(bool) return resolve();
                
                Fuse.configure(function(err)
                {
                    if(err)
                    {
                        console.error('error: FUSE is not available or loaded into the kernel.');
                        console.error('       Run as root to automatically do this for you.');
                        console.error(err);
                        process.exit(1);
                    }
                    else
                    {
                        console.log('Automatically loaded FUSE module into kernel.');
                        resolve();
                    }
                });
            });
        });
    }
    catch(err)
    {
        console.error('error: Failed to check if FUSE is available.');
        console.error(err);
        process.exit(1);
    }
    
    var context = {};
    
    // first connect to the server, if connection is broken, that means the FUSE mount will automatically unmount
    context.socket = net.createConnection(remotePort, remoteHostname);
    context.socket.on('error', function(err)
    {
        console.error('error: Connection with remote server failed (' + remoteHostname + ' ' + remotePort + ').');
        console.error(err);
        
        // 'close' is emitted right after 'error'
    });
    context.socket.on('close', function(hadError)
    {
        process.exit(hadError ? 1 : 0);
    });
    context.server = readline.createInterface({
        input: context.socket
    });
    
    context.writer = {
        write: function(obj)
        {
            context.socket.write(JSON.stringify(obj) + '\n');
        }
    };
    context.reader = {
        read: (function()
        {
            const getLineGen = (async function* ()
            {
                for await(const line of context.server)
                {
                    yield line;
                }
            })();
            return async () => {
                try
                {
                    return JSON.parse((await getLineGen.next()).value);
                }
                catch(err)
                {
                    return {
                        code: 'EPROTO' // protocol error
                    };
                }
            };
        })()
    };
    
    // nonempty: true -> force mount if directory is non-empty, which would otherwise warn that files within the target directory may be hidden
    // force: true -> unmount existing mount before mounting
    var fuse = new Fuse(
        mountPath,
        fuseOptionsFn.call(context),
        {
            debug: true,
            nonempty: true,
            force: true
        }
    );
    
    console.log('Mounting remote filesystem at: ' + mountPath);
    fuse.mount(err =>
    {
        if(err)
        {
            console.error(err);
            process.exit(1);
        }
        else
        {
            // automatically clean up this fuse mount later on exit:
            process.on('cleanup', () =>
            {
                try
                {
                    console.log('Unmounting ' + realMountPath);
                    
                    // note: no async in cleanup
                    Mount.umountSync(realMountPath);
                }
                catch(err)
                {
                    console.error(
                            'error: Could not unmount fuse mount directory (' + realMountPath + ').\n'
                        + '       Manual cleanup required (possibly with force):\n'
                        + '       [sudo] umount "' + realMountPath + '"\n'
                    );
                    console.error(err);
                }
            });
            
            console.log('Successfully mounted.');
        }
    });
}

if(require.main === module)
{
    process.on('EXIT', function exit_handler()
    {
        process.emit('cleanup');
    });
    process.on('SIGINT', function sigint_handler()
    {
        process.exit(0);
    });
    process.on('uncaughtException', function uncaughtException_handler(e)
    {
        console.error('error: Uncaught exception:');
        console.error(e.stack);
        process.exit(1);
    });
    
    const options = {};
    for(var i=2;i<process.argv.length;++i)
    {
        const arg = process.argv[i];
        
        if(arg === '--' || arg.startsWith('-'))
        {
            // handle command-line options
        }
        else
        {
            break;
        }
    }
    
    if(i < process.argv.length)
    {
        options.mountPath = process.argv[i++];
    }
    
    if(i < process.argv.length)
    {
        options.remoteHostname = process.argv[i++];
    }
    
    if(i < process.argv.length)
    {
        options.remotePort = parseInt(process.argv[i++]);
    }
    
    client(options);
}
else
{
    module.exports = client;
}
