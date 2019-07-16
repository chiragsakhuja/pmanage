'use strict';

const os = require('os');
const fs = require('fs');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const process = require('process');
const minimist = require('minimist');

let total_slots = 0;
let avail_slots = 0;
let start_time = new Date();
let pending_tasks = new Array();
let thread_end_times = new Array();
let uptime_interval = null;

function getTimestamp(time) {
    function zeroPadString(num) {
        return (num.toString().length < 2 ? '0' : '') + num.toString();
    }

    let date = zeroPadString(time.getDate());
    let month = zeroPadString(time.getMonth() + 1);
    let year = zeroPadString(time.getFullYear());
    let hour = zeroPadString(time.getHours());
    let minute = zeroPadString(time.getMinutes());
    let second = zeroPadString(time.getSeconds());
    return '[' + date + '-' + month + '-' + year + ' ' + hour + ':' + minute + ':' + second + ']';
}

function getDurationString(seconds) {
    let ret = '';
    if(seconds >= 3600) {
        ret += Math.floor(seconds / 3600) + 'h';
        seconds %= 3600;
    }
    if(seconds >= 60) {
        ret += Math.floor(seconds / 60) + 'm';
        seconds %= 60;
    }
    ret += seconds.toFixed(2) + 's';
    return ret;
}

async function asyncExec(command) {
    let {stdout, stderr} = await exec(command);
    return stdout.replace(/(\r\n|\n|\r)/gm, '');
}

async function begin() {
    let output = await asyncExec('uname -a');
    console.log(getTimestamp(new Date()) + ' ' + output);
}

async function uptime() {
    let output = await asyncExec('uptime');
    console.log(getTimestamp(new Date()) + ' ' + output);
}

function fin() {
    let end_time = new Date();
    let total_duration = end_time - start_time;

    let unused_area = 0;
    for(let thread_end_time of thread_end_times) {
        unused_area += end_time - thread_end_time;
    }
    let total_area = total_duration * total_slots;

    console.log(getTimestamp(end_time) + ' completed execution in ' + getDurationString(total_duration / 1000.0));
    console.log(getTimestamp(end_time) + ' utilization ' + Number.parseFloat(((total_area - unused_area) / total_area * 100)).toFixed(2) + '%');

    clearInterval(uptime_interval);
}

async function acquireSlot() {
    if(avail_slots > 0) {
        // if a slot is available, occupy it immediately
        --avail_slots;
        return true;
    } else {
        // if no slot is available, create a promise that will be resolved on completion of
        // a task that is currently running
        let slot_promise = new Promise((resolve, reject) => {
            pending_tasks.push(resolve);
        });
        return slot_promise;
    }
}

function releaseSlot() {
    if(pending_tasks.length > 0) {
        // if any tasks are pending, wake one of them
        let acquire_resolve = pending_tasks.shift();
        acquire_resolve();
    } else {
        // if no tasks are pending, just free the slot
        thread_end_times.push(new Date());
        ++avail_slots;
        if(avail_slots == total_slots) {
            fin();
        }
    }
}

function schedule(tasks)
{
    let total_tasks = tasks.length;
    let completed_tasks = 0;

    tasks.forEach(async (task, idx) => {
        // wait for a slot and sleep if none are available
        let slot_promise = await acquireSlot();

        // execute the task
        console.log(getTimestamp(new Date()) + ' (' + (idx + 1) + '/' + total_tasks + ') starting `' + task + '`');
        let workload_promise = exec(task);

        let start = process.hrtime();
        let {stdout, stderr} = await workload_promise;
        let end = process.hrtime(start);

        // once the command has executed, output the results and free a slot
        console.log(getTimestamp(new Date()) + ' (' + (completed_tasks + 1) + '/' + total_tasks + ') finished `' + task + '` in ' + getDurationString(end[0]));
        ++completed_tasks;
        if(stdout) {
            console.log('=== stdout');
            console.log(stdout);
            console.log('===');
        }
        if(stderr) {
            console.log('=== stderr');
            console.log(stderr);
            console.log('===');
        }

        releaseSlot();
    });
}

function usage() {
    console.log('usage: ' + process.argv[1] + ' -w workload [OPTIONS]');
    console.log();
    console.log('  -n N          number of cores [default N=all cores]');
    console.log('  -w W          workload list');
    console.log('  --ARG=VAL     replace @ARG with VAL in workload list');
}

function setupWorkload(wl_filename, args) {
    let contents = fs.readFileSync(wl_filename, 'utf8');
    let tasks = contents.split(os.EOL).filter((value, idx, array) => { return value[0] != '#'; });
    tasks.pop();

    // extract defines from workload list
    let defines = new Set();
    for(let line of tasks) {
        let cur_defines = line.match(/@[A-Z]+/g);
        if(cur_defines !== null) {
            for(let cur_define of cur_defines) {
                defines.add(cur_define.slice(1));
            }
        }
    }

    // check that all defines have been supplied
    for(let define of defines) {
        if(! (define in args)) {
            usage();
            console.log('must supply define ' + define);
            process.exit(1);
        }
    }

    // replace defines with actual values and attach extra arguments
    for(let i = 0; i < tasks.length; i += 1) {
        for(let define of defines) {
            let define_val = args[define].replace(/,/g, ' ');
            tasks[i] = tasks[i].replace(new RegExp('@' + define, 'g'), define_val);
        }
    }

    return tasks;
}

function main() {
    let args = minimist(process.argv.slice(2));

    if(! ('w' in args)) {
        console.log('must supply workload list');
        usage();
        process.exit(1);
    }

    if(! ('n' in args)) {
        avail_slots = os.cpus().length;
    } else {
        avail_slots = args['n'];
    }
    total_slots = avail_slots;

    begin();
    let tasks = setupWorkload(args['w'], args);
    uptime_interval = setInterval(uptime, 300000);
    schedule(tasks);
}

if(require.main === module) {
    main();
}
