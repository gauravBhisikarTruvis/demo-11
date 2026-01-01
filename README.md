sudo chmod -R o+rx /opt/app/retry-test/qg-backend
sudo find /opt/app/retry-test/qg-backend -type f -exec chmod o+r {} \;


sudo -u gaurav_bhisikar_hsbc_co_in \
  ls /opt/app/retry-test/qg-backend/src/services
