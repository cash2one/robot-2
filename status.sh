#ls -l hosts.bf
du -sh hosts.ldb/
du -sh hosts/queue.set/
du -sh hosts/ignored.set/
wc -l hosts/queue
echo $(cat hosts/.pos.queue)/$(stat -c "%s" hosts/queue)
ps uxf | grep robot2 | wc
