PKG_NAME:=oxidns
PKG_VERSION:=0.1.0
PKG_RELEASE:=1

include $(TOPDIR)/rules.mk
include $(INCLUDE_DIR)/package.mk

PKG_BUILD_PARALLEL:=1

define Package/oxidns
  SECTION:=net
  CATEGORY:=Network
  TITLE:=Oxidns DNS server
  URL:=https://github.com/809280470/oxidns
  DEPENDS:=
endef

define Package/oxidns/description
  Oxidns — high performance DNS server.
endef

define Build/Prepare
	mkdir -p $(PKG_BUILD_DIR)
	# copy prepared files from package files/ into build dir
	# files/ should contain usr/bin/oxidns, etc/init.d/oxidns, etc/config/oxidns
	$(CP) -a $(CURDIR)/files/* $(PKG_BUILD_DIR)/
endef

define Build/Compile
	# no compile here — binary provided in files/
endef

define Package/oxidns/install
	$(INSTALL_DIR) $(1)/usr/bin
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/usr/bin/oxidns $(1)/usr/bin/oxidns

	$(INSTALL_DIR) $(1)/etc/init.d
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/etc/init.d/oxidns $(1)/etc/init.d/oxidns

	$(INSTALL_DIR) $(1)/etc/config
	$(INSTALL_DATA) $(PKG_BUILD_DIR)/etc/config/oxidns $(1)/etc/config/oxidns

	# optional webui or other files
	-$(CP) -a $(PKG_BUILD_DIR)/www $(1)/usr/share/oxidns || true
endef

$(eval $(call BuildPackage,oxidns))
