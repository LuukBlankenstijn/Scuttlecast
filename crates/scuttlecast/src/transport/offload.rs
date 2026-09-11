use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::RawFd;
use std::ptr;

use socket2::SockAddr;

#[repr(align(8))]
struct Control([u8; 64]);

impl Control {
    fn new() -> Self {
        Self([0; 64])
    }
}

fn segment_size_space() -> libc::size_t {
    unsafe { libc::CMSG_SPACE(size_of::<u16>() as libc::c_uint) as libc::size_t }
}

pub fn send_segmented(
    fd: RawFd,
    body: &[u8],
    segment_size: usize,
    to: &SockAddr,
) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: body.as_ptr().cast_mut().cast(),
        iov_len: body.len(),
    };
    let mut control = Control::new();

    let sent = unsafe {
        let mut header: libc::msghdr = zeroed();
        header.msg_name = to.as_ptr().cast_mut().cast();
        header.msg_namelen = to.len();
        header.msg_iov = &mut iov;
        header.msg_iovlen = 1;
        header.msg_control = control.0.as_mut_ptr().cast();
        header.msg_controllen = segment_size_space();

        let cmsg = libc::CMSG_FIRSTHDR(&header);
        (*cmsg).cmsg_level = libc::IPPROTO_UDP;
        (*cmsg).cmsg_type = libc::UDP_SEGMENT;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<u16>() as libc::c_uint) as libc::size_t;
        ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<u16>(), segment_size as u16);

        libc::sendmsg(fd, &header, 0)
    };

    if sent < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(sent as usize)
}

pub fn enable_coalescing(fd: RawFd) -> io::Result<()> {
    let on: libc::c_int = 1;
    let set = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_UDP,
            libc::UDP_GRO,
            ptr::addr_of!(on).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    };

    if set < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

pub fn recv_coalesced(fd: RawFd, buf: &mut [u8]) -> io::Result<(usize, Option<usize>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    let mut control = Control::new();

    let (received, segment_size) = unsafe {
        let mut header: libc::msghdr = zeroed();
        header.msg_iov = &mut iov;
        header.msg_iovlen = 1;
        header.msg_control = control.0.as_mut_ptr().cast();
        header.msg_controllen = control.0.len() as libc::size_t;

        let received = libc::recvmsg(fd, &mut header, 0);
        if received < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut segment_size = None;
        let mut cmsg = libc::CMSG_FIRSTHDR(&header);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::IPPROTO_UDP && (*cmsg).cmsg_type == libc::UDP_GRO {
                segment_size =
                    Some(ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::c_int>()) as usize);
            }
            cmsg = libc::CMSG_NXTHDR(&header, cmsg);
        }

        (received as usize, segment_size)
    };

    Ok((received, segment_size.filter(|stride| *stride > 0)))
}
