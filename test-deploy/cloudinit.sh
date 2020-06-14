#cloud-config

apt:
  sources:
    source_dokku: 
      source: 'deb https://packagecloud.io/dokku/dokku/ubuntu/ $RELEASE main'
      key: |
        -----BEGIN PGP PUBLIC KEY BLOCK-----
        Version: GnuPG v1.4.11 (GNU/Linux)

        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
        =laU4
        -----END PGP PUBLIC KEY BLOCK-----
    source_docker: 
      source: 'deb [arch=amd64] https://download.docker.com/linux/ubuntu $RELEASE stable'
      key: |
        -----BEGIN PGP PUBLIC KEY BLOCK-----

        mQINBFit2ioBEADhWpZ8/wvZ6hUTiXOwQHXMAlaFHcPH9hAtr4F1y2+OYdbtMuth
        lqqwp028AqyY+PRfVMtSYMbjuQuu5byyKR01BbqYhuS3jtqQmljZ/bJvXqnmiVXh
        38UuLa+z077PxyxQhu5BbqntTPQMfiyqEiU+BKbq2WmANUKQf+1AmZY/IruOXbnq
        L4C1+gJ8vfmXQt99npCaxEjaNRVYfOS8QcixNzHUYnb6emjlANyEVlZzeqo7XKl7
        UrwV5inawTSzWNvtjEjj4nJL8NsLwscpLPQUhTQ+7BbQXAwAmeHCUTQIvvWXqw0N
        cmhh4HgeQscQHYgOJjjDVfoY5MucvglbIgCqfzAHW9jxmRL4qbMZj+b1XoePEtht
        ku4bIQN1X5P07fNWzlgaRL5Z4POXDDZTlIQ/El58j9kp4bnWRCJW0lya+f8ocodo
        vZZ+Doi+fy4D5ZGrL4XEcIQP/Lv5uFyf+kQtl/94VFYVJOleAv8W92KdgDkhTcTD
        G7c0tIkVEKNUq48b3aQ64NOZQW7fVjfoKwEZdOqPE72Pa45jrZzvUFxSpdiNk2tZ
        XYukHjlxxEgBdC/J3cMMNRE1F4NCA3ApfV1Y7/hTeOnmDuDYwr9/obA8t016Yljj
        q5rdkywPf4JF8mXUW5eCN1vAFHxeg9ZWemhBtQmGxXnw9M+z6hWwc6ahmwARAQAB
        tCtEb2NrZXIgUmVsZWFzZSAoQ0UgZGViKSA8ZG9ja2VyQGRvY2tlci5jb20+iQI3
        BBMBCgAhBQJYrefAAhsvBQsJCAcDBRUKCQgLBRYCAwEAAh4BAheAAAoJEI2BgDwO
        v82IsskP/iQZo68flDQmNvn8X5XTd6RRaUH33kXYXquT6NkHJciS7E2gTJmqvMqd
        tI4mNYHCSEYxI5qrcYV5YqX9P6+Ko+vozo4nseUQLPH/ATQ4qL0Zok+1jkag3Lgk
        jonyUf9bwtWxFp05HC3GMHPhhcUSexCxQLQvnFWXD2sWLKivHp2fT8QbRGeZ+d3m
        6fqcd5Fu7pxsqm0EUDK5NL+nPIgYhN+auTrhgzhK1CShfGccM/wfRlei9Utz6p9P
        XRKIlWnXtT4qNGZNTN0tR+NLG/6Bqd8OYBaFAUcue/w1VW6JQ2VGYZHnZu9S8LMc
        FYBa5Ig9PxwGQOgq6RDKDbV+PqTQT5EFMeR1mrjckk4DQJjbxeMZbiNMG5kGECA8
        g383P3elhn03WGbEEa4MNc3Z4+7c236QI3xWJfNPdUbXRaAwhy/6rTSFbzwKB0Jm
        ebwzQfwjQY6f55MiI/RqDCyuPj3r3jyVRkK86pQKBAJwFHyqj9KaKXMZjfVnowLh
        9svIGfNbGHpucATqREvUHuQbNnqkCx8VVhtYkhDb9fEP2xBu5VvHbR+3nfVhMut5
        G34Ct5RS7Jt6LIfFdtcn8CaSas/l1HbiGeRgc70X/9aYx/V/CEJv0lIe8gP6uDoW
        FPIZ7d6vH+Vro6xuWEGiuMaiznap2KhZmpkgfupyFmplh0s6knymuQINBFit2ioB
        EADneL9S9m4vhU3blaRjVUUyJ7b/qTjcSylvCH5XUE6R2k+ckEZjfAMZPLpO+/tF
        M2JIJMD4SifKuS3xck9KtZGCufGmcwiLQRzeHF7vJUKrLD5RTkNi23ydvWZgPjtx
        Q+DTT1Zcn7BrQFY6FgnRoUVIxwtdw1bMY/89rsFgS5wwuMESd3Q2RYgb7EOFOpnu
        w6da7WakWf4IhnF5nsNYGDVaIHzpiqCl+uTbf1epCjrOlIzkZ3Z3Yk5CM/TiFzPk
        z2lLz89cpD8U+NtCsfagWWfjd2U3jDapgH+7nQnCEWpROtzaKHG6lA3pXdix5zG8
        eRc6/0IbUSWvfjKxLLPfNeCS2pCL3IeEI5nothEEYdQH6szpLog79xB9dVnJyKJb
        VfxXnseoYqVrRz2VVbUI5Blwm6B40E3eGVfUQWiux54DspyVMMk41Mx7QJ3iynIa
        1N4ZAqVMAEruyXTRTxc9XW0tYhDMA/1GYvz0EmFpm8LzTHA6sFVtPm/ZlNCX6P1X
        zJwrv7DSQKD6GGlBQUX+OeEJ8tTkkf8QTJSPUdh8P8YxDFS5EOGAvhhpMBYD42kQ
        pqXjEC+XcycTvGI7impgv9PDY1RCC1zkBjKPa120rNhv/hkVk/YhuGoajoHyy4h7
        ZQopdcMtpN2dgmhEegny9JCSwxfQmQ0zK0g7m6SHiKMwjwARAQABiQQ+BBgBCAAJ
        BQJYrdoqAhsCAikJEI2BgDwOv82IwV0gBBkBCAAGBQJYrdoqAAoJEH6gqcPyc/zY
        1WAP/2wJ+R0gE6qsce3rjaIz58PJmc8goKrir5hnElWhPgbq7cYIsW5qiFyLhkdp
        YcMmhD9mRiPpQn6Ya2w3e3B8zfIVKipbMBnke/ytZ9M7qHmDCcjoiSmwEXN3wKYI
        mD9VHONsl/CG1rU9Isw1jtB5g1YxuBA7M/m36XN6x2u+NtNMDB9P56yc4gfsZVES
        KA9v+yY2/l45L8d/WUkUi0YXomn6hyBGI7JrBLq0CX37GEYP6O9rrKipfz73XfO7
        JIGzOKZlljb/D9RX/g7nRbCn+3EtH7xnk+TK/50euEKw8SMUg147sJTcpQmv6UzZ
        cM4JgL0HbHVCojV4C/plELwMddALOFeYQzTif6sMRPf+3DSj8frbInjChC3yOLy0
        6br92KFom17EIj2CAcoeq7UPhi2oouYBwPxh5ytdehJkoo+sN7RIWua6P2WSmon5
        U888cSylXC0+ADFdgLX9K2zrDVYUG1vo8CX0vzxFBaHwN6Px26fhIT1/hYUHQR1z
        VfNDcyQmXqkOnZvvoMfz/Q0s9BhFJ/zU6AgQbIZE/hm1spsfgvtsD1frZfygXJ9f
        irP+MSAI80xHSf91qSRZOj4Pl3ZJNbq4yYxv0b1pkMqeGdjdCYhLU+LZ4wbQmpCk
        SVe2prlLureigXtmZfkqevRz7FrIZiu9ky8wnCAPwC7/zmS18rgP/17bOtL4/iIz
        QhxAAoAMWVrGyJivSkjhSGx1uCojsWfsTAm11P7jsruIL61ZzMUVE2aM3Pmj5G+W
        9AcZ58Em+1WsVnAXdUR//bMmhyr8wL/G1YO1V3JEJTRdxsSxdYa4deGBBY/Adpsw
        24jxhOJR+lsJpqIUeb999+R8euDhRHG9eFO7DRu6weatUJ6suupoDTRWtr/4yGqe
        dKxV3qQhNLSnaAzqW/1nA3iUB4k7kCaKZxhdhDbClf9P37qaRW467BLCVO/coL3y
        Vm50dwdrNtKpMBh3ZpbB1uJvgi9mXtyBOMJ3v8RZeDzFiG8HdCtg9RvIt/AIFoHR
        H3S+U79NT6i0KPzLImDfs8T7RlpyuMc4Ufs8ggyg9v3Ae6cN3eQyxcK3w0cbBwsh
        /nQNfsA6uu+9H7NhbehBMhYnpNZyrHzCmzyXkauwRAqoCbGCNykTRwsur9gS41TQ
        M8ssD1jFheOJf3hODnkKU+HKjvMROl1DK7zdmLdNzA1cvtZH/nCC9KPj1z8QC47S
        xx+dTZSx4ONAhwbS/LN3PoKtn8LPjY9NP9uDWI+TWYquS2U+KHDrBDlsgozDbs/O
        jCxcpDzNmXpWQHEtHU7649OXHP7UeNST1mCUCH5qdank0V1iejF6/CfTFU4MfcrG
        YT90qFF93M3v01BbxP+EIY2/9tiIPbrd
        =0YYh
        -----END PGP PUBLIC KEY BLOCK-----

  debconf_selections:
    web:      dokku dokku/web_config boolean false
    vhost:    dokku dokku/vhost_enable boolean true
    # set the domain name of the new Dokku server
    hostname: dokku dokku/hostname string projecttycho.nl
    # this copies over the public SSH key assigned to the server
    key:      dokku dokku/key_file string /root/.ssh/authorized_keys

packages:
  - dokku
  # packages needed for local workers
  - ccache
  - libsdl2-dev
package_update: true
package_upgrade: true
# TODO: back to true when it works.
package_reboot_if_required: false

runcmd:
  - [ 'dokku', 'plugin:install', 'https://github.com/dokku/dokku-postgres.git' ]
  - [ 'dokku', 'plugin:install', 'https://github.com/dokku/dokku-letsencrypt.git' ]