import {Webstore} from '../index';

describe('Webstore unit test', () => {
  let webstore;
  beforeEach(() => {
    webstore = new Webstore();
  });

  it('basic crud', () => {
    const docRef = webstore.getRef('collection1', 'doc1');
    // doc not exists
    expect(docRef.get('$.name').apply()).toBeFalsy();

    // set
    docRef.set('$.name', 'moon').apply();
    expect(docRef.get('$.name').apply()).toEqual('moon');

    // update
    docRef.set('$.name', 'sun').apply();
    expect(docRef.get('$.name').apply()).toEqual('sun');

    // move
    docRef.move('$.name', '$.newName').apply();
    expect(docRef.get('$.newName').apply()).toEqual('sun');
    expect(docRef.get('$.name').apply()).toBeFalsy();

    // delete
    docRef.delete('$.newName', 'sun').apply();
    expect(docRef.get('$.newName').apply()).toBeFalsy();
  });

  it('watch', () => {
    const docRef = webstore.getRef('collection1', 'doc1');
    let watchValue;
    docRef.set('$.name', 'moon').apply();
    const remover = docRef.watch('$.name', (v) => {
      watchValue = v;
    });
    expect(watchValue).toBe('moon');

    docRef.set('$.name', 'sun').apply();
    expect(watchValue).toBe('sun');
    remover();
  });

  it('two watches', () => {
    // given
    const docRef = webstore.getRef('collection1', 'doc1');
    let watchValue1;
    let watchValue2;
    docRef.set('$.name', 'moon').apply();

    // when to watch created for the same doc, jsonpath
    const remover1 = docRef.watch('$.name', (v) => {
      watchValue1 = v;
    });
    const remover2 = docRef.watch('$.name', (v) => {
      watchValue2 = v;
    });

    expect(watchValue1).toBe('moon');
    expect(watchValue2).toBe('moon');

    docRef.set('$.name', 'sun').apply();
    expect(watchValue1).toBe('sun');
    expect(watchValue2).toBe('sun');

    remover1();
    remover2();
  });

  it('watcher re-registration', () => {
    // given
    const docRef = webstore.getRef('collection1', 'doc1');
    let watchValue1;
    let watchValue2;
    docRef.set('$.name', 'moon').apply();

    const remover1 = docRef.watch('$.name', (v) => {
      watchValue1 = v;
    });
    const remover2 = docRef.watch('$.name', (v) => {
      watchValue2 = v;
    });

    expect(watchValue1).toBe('moon');
    expect(watchValue2).toBe('moon');

    // when reconnection happens
    watchValue1 = watchValue2 = undefined;
    webstore.rpc.watches = {};  // simmulate disconnect
    webstore.registerAllWatches();      // re-register all watches

    // then watch keep working
    expect(watchValue1).toBe('moon');
    expect(watchValue2).toBe('moon');

    docRef.set('$.name', 'sun').apply();
    expect(watchValue1).toBe('sun');
    expect(watchValue2).toBe('sun');

    remover1();
    remover2();
  });

  it('watcher re-registration and remove', () => {
    // given
    const docRef = webstore.getRef('collection1', 'doc1');
    docRef.set('$.name', 'moon').apply();
    expect(Object.keys(webstore.rpc.watches).length).toBe(0);

    const remover1 = docRef.watch('$.name', (v) => {});
    expect(Object.keys(webstore.rpc.watches).length).toBe(1);
    const remover2 = docRef.watch('$.name', (v) => {});
    expect(Object.keys(webstore.rpc.watches).length).toBe(1);

    // reconnection happens
    webstore.rpc.watches = {};  // simmulate disconnect
    webstore.registerAllWatches();      // re-register all watches

    // should be re-registered
    expect(Object.keys(webstore.rpc.watches).length).toBe(1);

    // remove watch1 after re-registration
    remover1();
    expect(Object.keys(webstore.rpc.watches).length).toBe(1);
    remover2();
    expect(Object.keys(webstore.rpc.watches).length).toBe(0);
  });
});
