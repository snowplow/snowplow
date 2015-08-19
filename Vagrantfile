Vagrant.configure("2") do |config|

  config.vm.box = "ubuntu/trusty64"
  config.vm.hostname = "snowplow"
  config.ssh.forward_agent = true

  # Required for NFS to work, pick any local IP
  # Use NFS for shared folders for better performance
  # config.vm.network :private_network, ip: '192.168.50.50' # Uncomment to use NFS
  # config.vm.synced_folder '.', '/vagrant', nfs: true # Uncomment to use NFS

  config.vm.provider :virtualbox do |vb|
    vb.name = Dir.pwd().split("/")[-1] + "-" + Time.now.to_f.to_i.to_s
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize [ "guestproperty", "set", :id, "--timesync-threshold", 10000 ]
    # Scala is memory-hungry
    vb.memory = 5120
    # vb.cpus = 4 # Uncomment to use more cores
  end

  config.vm.provision :shell do |sh|
    sh.path = "vagrant/up.bash"
  end

  # Requires Vagrant 1.7.0+
  config.push.define "assetsync", strategy: "local-exec" do |push|
    push.script = "vagrant/push/sync-hosted-assets.bash"
  end
  config.push.define "kinesis", strategy: "local-exec" do |push|
    push.script = "vagrant/push/publish-kinesis-release.bash"
  end
  config.push.define "emr", strategy: "local-exec" do |push|
    push.script = "vagrant/push/publish-emr-release.bash"
  end

end
