#include <sys/ptrace.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <signal.h>

#include "zdtmtst.h"

const char *test_doc	= "Check ptrace, if the child process's stopped by signal";
const char *test_author	= "Andrey Vagin <avagin@parallels.com>";

typedef void (*sighandler_t)(int);

int child_fd;
int child_exit = 0;
void sig_handler(int signo, siginfo_t *siginfo, void *data)
{
	int pid, ret;
	test_msg("receive signal sig=%d from pid=%d\n", signo, siginfo->si_pid);
	pid = siginfo->si_pid;
	ret = write(child_fd, &pid, sizeof(pid));
	if (ret != sizeof(pid))
		err("write");
	child_exit = 1;
}

void sig_chld_handler(int signo)
{
	test_msg("Receive signal %d\n", signo);
}

int child(int fd)
{
	int ret = 0;
	struct sigaction act = {
		.sa_sigaction = sig_handler,
		.sa_flags = SA_SIGINFO,
	}, old_act;

	sigemptyset(&act.sa_mask);

	child_fd = fd;

	ret = sigaction(SIGUSR2, &act, &old_act);
	if (ret < 0) {
		err("signal failed\n");
		return 1;
	}

	ret = ptrace(PTRACE_TRACEME, 0, 0, 0);
	if (ret < 0) {
		err("ptrace failed\n");
		return 1;
	}
	ret = write(child_fd, &ret, sizeof(ret));
	while (!child_exit)
		ret = sleep(1);
	close(child_fd);
	return 0;
}

int main(int argc, char ** argv)
{
	int ret, status = 0;
	pid_t pid, spid, cpid;
	sighandler_t sh;
	int signal_pipe[2];
	int child_pipe[2];

	test_init(argc, argv);

	ret = pipe(child_pipe);
	if (ret < 0) {
		err("pipe failed");
		return 1;
	}

	cpid = test_fork();
	if (cpid < 0) {
		err("fork failed");
		return 1;
	}
	else if (cpid == 0) {
		close(child_pipe[0]);
		return child(child_pipe[1]);
	}

	close(child_pipe[1]);
	ret = pipe(signal_pipe);
	if (ret < 0) {
		err("pipe failed");
		return 1;
	}

	spid = test_fork();
	if (spid < 0) {
		err("Can't fork signal process");
		return 1;
	} else if (spid == 0) {
		close(signal_pipe[1]);
		ret = read(signal_pipe[0], &status, sizeof(status));
		if (ret != sizeof(status)) {
			err("read");
			return 1;
		}
		test_msg("send signal to %d\n", cpid);
		ret = kill(cpid, SIGUSR2);
		if (ret < 0) {
			err("kill failed");
		}
		return 0;
	}
	close(signal_pipe[0]);

	sh = signal(SIGCHLD, sig_chld_handler);
	if (sh == SIG_ERR) {
		err("signal failed");
		return 1;
	}

	test_msg("wait while child initialized");
	ret = read(child_pipe[0], &status, sizeof(status));
	if  (ret != sizeof(status)) {
		err("read from child process failed\n");
		return 1;
	}

	ret = write(signal_pipe[1], &status, sizeof(status));
	if (ret != sizeof(status)) {
		err("write to signal process failed");
	}
	close(signal_pipe[1]);

	test_daemon();
	test_waitsig();

	while (1) {
		test_msg("waiting...\n");
		pid = wait(&status);
		if (pid < 0) {
			if (errno != ECHILD)
				err("wait");
			break;
		}

		if (WIFSTOPPED(status)) {
			siginfo_t siginfo;

			test_msg("pid=%d stopsig=%d\n", pid, WSTOPSIG(status));

			ret = ptrace(PTRACE_GETSIGINFO, pid, 0, &siginfo);
			if (ret < 0) {
				err("ptrace failed");
				return 1;
			} else
				test_msg("pid=%d sends signal\n", siginfo.si_pid);

			ret = ptrace(PTRACE_CONT, pid, 0, WSTOPSIG(status));
			if (ret < 0)
				err("ptrace failed");

			ret = read(child_pipe[0], &status, sizeof(status));
			if  (ret != sizeof(status)) {
				err("read");
				return 1;
			}

			if (spid != siginfo.si_pid)
				fail("%d!=%d", cpid, siginfo.si_pid);
			else if (status == siginfo.si_pid)
				pass();
			else {
				fail("%d!=%d", status, siginfo.si_pid);
				return 1;
			}
		}
		if (WIFEXITED(status)) {
			test_msg("pid = %d status = %d\n", pid, WEXITSTATUS(status));
			if (WEXITSTATUS(status))
				return 1;
		}
		if (WIFSTOPPED(status)) {
			test_msg("pid = %d signal = %d\n", pid, WTERMSIG(status));
			return 1;
		}
	}
	return 0;
}
